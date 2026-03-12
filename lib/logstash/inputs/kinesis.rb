# encoding: utf-8

require "socket"
require "uri"
require "logstash/inputs/base"
require "logstash/errors"
require "logstash/environment"
require "logstash/namespace"

require 'logstash-input-kinesis_jars'

# Receive events through an AWS Kinesis stream.
#
# This input plugin uses the Java Kinesis Client Library v2 underneath, so the
# documentation at https://github.com/awslabs/amazon-kinesis-client will be
# useful.
#
# AWS credentials can be specified either through environment variables, or an
# IAM instance role. The library uses a DynamoDB table for worker coordination,
# so you'll need to grant access to that as well as to the Kinesis stream. The
# DynamoDB table has the same name as the `application_name` configuration
# option, which defaults to "logstash".
#
# The library can optionally also send worker statistics to CloudWatch.
class LogStash::Inputs::Kinesis < LogStash::Inputs::Base
  KCL = Java::SoftwareAmazonKinesisCoordinator
  Records = Java::SoftwareAmazonKinesisRetrievalRecords
  ClientConfig = Java::SoftwareAmazonKinesisCommon
  require "logstash/inputs/kinesis/worker"

  config_name 'kinesis'

  attr_reader(
    :kcl_config,
    :kcl_worker,
  )

  # The application name used for the dynamodb coordination table. Must be
  # unique for this kinesis stream.
  config :application_name, :validate => :string, :default => "logstash"

  # The kinesis stream name.
  config :kinesis_stream_name, :validate => :string, :required => true

  # The AWS region for Kinesis, DynamoDB, and CloudWatch (if enabled)
  config :region, :validate => :string, :default => "us-east-1"

  # How many seconds between worker checkpoints to dynamodb.
  config :checkpoint_interval_seconds, :validate => :number, :default => 60

  # Worker metric tracking. By default this is disabled, set it to "cloudwatch"
  # to enable the cloudwatch integration in the Kinesis Client Library.
  config :metrics, :validate => [nil, "cloudwatch"], :default => nil

  # Select AWS profile for input
  config :profile, :validate => :string

  # The AWS IAM Role to assume, if any.
  # This is used to generate temporary credentials typically for cross-account access.
  # See https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html for more information.
  config :role_arn, :validate => :string

  # Session name to use when assuming an IAM role
  config :role_session_name, :validate => :string, :default => "logstash"

  # Select initial_position_in_stream. Accepts TRIM_HORIZON or LATEST
  config :initial_position_in_stream, :validate => ["TRIM_HORIZON", "LATEST"], :default => "TRIM_HORIZON"

  # Any additional KCL options. Settings are applied to the appropriate
  # sub-config object (CheckpointConfig, CoordinatorConfig, LeaseManagementConfig,
  # LifecycleConfig, MetricsConfig, ProcessorConfig, or RetrievalConfig).
  # Use camelCase or snake_case keys matching the Java method names.
  config :additional_settings, :validate => :hash, :default => {}

  # Kinesis endpoint override (for LocalStack or custom endpoints)
  config :kinesis_endpoint, :validate => :string, :default => nil

  # DynamoDB endpoint override (for LocalStack or custom endpoints)
  config :dynamodb_endpoint, :validate => :string, :default => nil

  # CloudWatch endpoint override (for LocalStack or custom endpoints)
  config :cloudwatch_endpoint, :validate => :string, :default => nil

  # Proxy for Kinesis, DynamoDB, and CloudWatch (if enabled)
  config :http_proxy, :validate => :password, :default => nil

  # Hosts that should be excluded from proxying
  config :non_proxy_hosts, :validate => :string, :default => nil

  def initialize(params = {})
    super(params)
  end

  def register
    # the INFO log level is extremely noisy in KCL
    lg = org.apache.commons.logging::LogFactory.getLog("software.amazon.kinesis")
    if lg.kind_of?(org.apache.commons.logging.impl::Jdk14Logger)
      kinesis_logger = lg.logger
      if kinesis_logger.kind_of?(java.util.logging::Logger)
        kinesis_logger.setLevel(java.util.logging::Level::WARNING)
      else
        kinesis_logger.setLevel(org.apache.log4j::Level::WARN)
      end
    elsif lg.kind_of?(org.apache.logging.log4jJcl::Log4jLog)
      logContext = org.apache.logging.log4j::LogManager.getContext(false)
      config = logContext.getConfiguration()
      config.getLoggerConfig("software.amazon.kinesis").setLevel(org.apache.logging.log4j::Level::WARN)
    else
      raise "Can't configure WARN log level for logger wrapper class #{lg.class}"
    end

    @logger.info("Registering logstash-input-kinesis")

    hostname = Socket.gethostname
    uuid = java.util::UUID.randomUUID.to_s
    worker_id = "#{hostname}:#{uuid}"

    # Build AWS SDK v2 credentials provider
    creds_provider_builder = Java::SoftwareAmazonAwssdkAuthCredentials::AwsCredentialsProviderChain.builder()

    # If the AWS profile is set, use the profile credentials provider.
    unless @profile.nil?
      profile_creds = Java::SoftwareAmazonAwssdkAuthCredentials::ProfileCredentialsProvider.builder()
        .profileName(@profile)
        .build()
      creds_provider_builder.addCredentialsProvider(profile_creds)
    end

    # Add default credential provider chain
    creds_provider_builder.addCredentialsProvider(
      Java::SoftwareAmazonAwssdkAuthCredentials::DefaultCredentialsProvider.create()
    )

    base_creds_provider = creds_provider_builder.build()

    # Build HTTP client configuration first (needed for STS if role_arn is set)
    region = Java::SoftwareAmazonAwssdkRegions::Region.of(@region)
    http_client_builder = Java::SoftwareAmazonAwssdkHttpApache::ApacheHttpClient.builder()
    
    if @http_proxy && !@http_proxy.value.to_s.strip.empty?
      proxy_uri = URI(@http_proxy.value)
      @logger.info("Using proxy #{proxy_uri.scheme}://#{proxy_uri.user}:*****@#{proxy_uri.host}:#{proxy_uri.port}")
      
      proxy_config_builder = Java::SoftwareAmazonAwssdkHttpApache::ProxyConfiguration.builder()
        .endpoint(java.net.URI.new("#{proxy_uri.scheme}://#{proxy_uri.host}:#{proxy_uri.port}"))

      proxy_config_builder.username(proxy_uri.user) if proxy_uri.user
      proxy_config_builder.password(proxy_uri.password) if proxy_uri.password
      if @non_proxy_hosts
        non_proxy_set = java.util.HashSet.new
        @non_proxy_hosts.split(',').map(&:strip).each { |host| non_proxy_set.add(host) }
        proxy_config_builder.nonProxyHosts(non_proxy_set)
      end
      
      http_client_builder.proxyConfiguration(proxy_config_builder.build())
    end

    http_client = http_client_builder.build()

    # If a role ARN is set then assume the role
    unless @role_arn.nil?
      sts_client = Java::SoftwareAmazonAwssdkServicesSts::StsClient.builder()
        .region(region)
        .credentialsProvider(base_creds_provider)
        .httpClient(http_client)
        .build()

      assume_role_request = Java::SoftwareAmazonAwssdkServicesStsModel::AssumeRoleRequest.builder()
        .roleArn(@role_arn)
        .roleSessionName(@role_session_name)
        .build()

      creds_provider = Java::SoftwareAmazonAwssdkServicesStsAuth::StsAssumeRoleCredentialsProvider.builder()
        .stsClient(sts_client)
        .refreshRequest(assume_role_request)
        .build()
    else
      creds_provider = base_creds_provider
    end

    # Create AWS SDK v2 clients for Kinesis, DynamoDB, and CloudWatch
    kinesis_builder = Java::SoftwareAmazonAwssdkServicesKinesis::KinesisAsyncClient.builder()
      .region(region)
      .credentialsProvider(creds_provider)
      .httpClient(http_client)
    kinesis_builder.endpointOverride(Java::JavaNet::URI.create(@kinesis_endpoint)) if @kinesis_endpoint
    kinesis_client = kinesis_builder.build()

    dynamo_builder = Java::SoftwareAmazonAwssdkServicesDynamodb::DynamoDbAsyncClient.builder()
      .region(region)
      .credentialsProvider(creds_provider)
      .httpClient(http_client)
    dynamo_builder.endpointOverride(Java::JavaNet::URI.create(@dynamodb_endpoint)) if @dynamodb_endpoint
    dynamo_client = dynamo_builder.build()

    cloudwatch_builder = Java::SoftwareAmazonAwssdkServicesCloudwatch::CloudWatchAsyncClient.builder()
      .region(region)
      .credentialsProvider(creds_provider)
      .httpClient(http_client)
    cloudwatch_builder.endpointOverride(Java::JavaNet::URI.create(@cloudwatch_endpoint)) if @cloudwatch_endpoint
    cloudwatch_client = cloudwatch_builder.build()

    initial_position = if @initial_position_in_stream == "TRIM_HORIZON"
      ClientConfig::InitialPositionInStreamExtended.newInitialPosition(ClientConfig::InitialPositionInStream::TRIM_HORIZON)
    else
      ClientConfig::InitialPositionInStreamExtended.newInitialPosition(ClientConfig::InitialPositionInStream::LATEST)
    end

    configsBuilder = ClientConfig::ConfigsBuilder.new(
      @kinesis_stream_name,
      @application_name,
      kinesis_client,
      dynamo_client,
      cloudwatch_client,
      worker_id,
      worker_factory_lambda()
    )

    # Apply additional settings to KCL sub-configuration objects.
    # In KCL v2, settings live on sub-config objects (e.g., leaseManagementConfig,
    # retrievalConfig) rather than on ConfigsBuilder directly.
    # We try each sub-config to find which one has the matching method.
    # Both camelCase and snake_case keys are supported (JRuby handles conversion).
    unless @additional_settings.empty?
      sub_configs = [
        configsBuilder,
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        configsBuilder.processorConfig(),
        configsBuilder.retrievalConfig(),
      ]

      @additional_settings.each do |key, value|
        applied = false
        sub_configs.each do |cfg|
          begin
            cfg.send(key, value)
            @logger.info("Applied additional setting", :key => key, :config => cfg.java_class.simple_name)
            applied = true
            break
          rescue NoMethodError
            next
          end
        end

        unless applied
          raise NoMethodError, "No matching KCL configuration property found for '#{key}'. " \
            "Settings must match a method on one of: ConfigsBuilder, CheckpointConfig, " \
            "CoordinatorConfig, LeaseManagementConfig, LifecycleConfig, MetricsConfig, " \
            "ProcessorConfig, or RetrievalConfig."
        end
      end
    end

    @kcl_config = configsBuilder
    @logger.info("Registered logstash-input-kinesis")
  end

  def run(output_queue)
    @output_queue_holder.queue = output_queue if @output_queue_holder
    @kcl_worker = KCL::Scheduler.new(
      @kcl_config.checkpointConfig(),
      @kcl_config.coordinatorConfig(),
      @kcl_config.leaseManagementConfig(),
      @kcl_config.lifecycleConfig(),
      @kcl_config.metricsConfig(),
      @kcl_config.processorConfig(),
      @kcl_config.retrievalConfig()
    )
    @kcl_worker.run()
  end

  def stop
    @kcl_worker.shutdown() if @kcl_worker
  end

  def worker_factory_lambda
    # Create a ShardRecordProcessorFactory that returns our Worker instances
    factory_class = Class.new do
      include Java::SoftwareAmazonKinesisProcessor::ShardRecordProcessorFactory
      
      def initialize(codec, output_queue_holder, decorator, checkpoint_interval, logger)
        super()
        @codec = codec
        @output_queue_holder = output_queue_holder
        @decorator = decorator
        @checkpoint_interval = checkpoint_interval
        @logger = logger
      end
      
      def shardRecordProcessor(shard_info = nil)
        Worker.new(
          @codec.clone,
          @output_queue_holder.queue,
          @decorator,
          @checkpoint_interval,
          @logger
        )
      end
    end
    
    # Use a holder object to allow late binding of output_queue
    output_queue_holder = Struct.new(:queue).new
    @output_queue_holder = output_queue_holder
    
    factory_class.new(
      @codec,
      output_queue_holder,
      method(:decorate),
      @checkpoint_interval_seconds,
      @logger
    )
  end
end
