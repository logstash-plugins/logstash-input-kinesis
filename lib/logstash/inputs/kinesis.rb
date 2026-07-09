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
# This input plugin uses the Java Kinesis Client Library (KCL) underneath, so the
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
  require "logstash/inputs/kinesis/worker"

  config_name 'kinesis'

  # AWS SDK for Java v2 and KCL 2.x are published under the `software.amazon`
  # package root. Unlike `com`/`org`/`java`, JRuby does not expose `software` as
  # a top-level constant, so these packages must be reached through `Java::`.
  AWS = Java::software.amazon.awssdk
  KCL = Java::software.amazon.kinesis

  attr_reader(
    :aws_credentials_provider,
    :kinesis_client,
    :dynamo_db_client,
    :cloud_watch_client,
    :checkpoint_config,
    :coordinator_config,
    :lease_management_config,
    :lifecycle_config,
    :metrics_config,
    :processor_config,
    :retrieval_config,
    :polling_config,
    :kcl_scheduler,
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

  # Advanced KCL tuning, grouped by the KCL 2.x configuration object the settings
  # apply to. Each group is a hash of `snake_case` setter => value pairs, e.g.:
  #
  #   additional_settings => {
  #     "lease_management_config" => { "failover_time_millis" => 15000 }
  #     "retrieval_config"        => { "list_shards_backoff_time_in_millis" => 3000 }
  #     "polling_config"          => { "max_records" => 5000 }
  #   }
  #
  # Grouping by config object lets the same setting take different values on
  # different objects (e.g. `list_shards_backoff_time_in_millis` exists on both
  # LeaseManagementConfig and RetrievalConfig). Valid groups: checkpoint_config,
  # coordinator_config, lease_management_config, lifecycle_config, metrics_config,
  # processor_config, retrieval_config, polling_config.
  #
  # For backward compatibility, the top-level keys `kinesis_endpoint` and
  # `dynamodb_endpoint` are also accepted and applied as AWS SDK v2 client
  # endpoint overrides.
  config :additional_settings, :validate => :hash, :default => {}

  # Proxy for Kinesis, DynamoDB, and CloudWatch (if enabled)
  config :http_proxy, :validate => :password, :default => nil

  # Hosts that should be excluded from proxying, separated by the "|" (pipe) character.
  config :non_proxy_hosts, :validate => :string, :default => nil

  def initialize(params = {})
    super(params)
  end

  def register
    # the INFO log level is extremely noisy in KCL; the library moved to the
    # software.amazon.* namespaces and logs through SLF4J (Log4j2 in Logstash).
    quiet_kcl_logging

    @logger.info("Registering logstash-input-kinesis")

    hostname = Socket.gethostname
    uuid = java.util::UUID.randomUUID.to_s
    @worker_id = "#{hostname}:#{uuid}"

    region = AWS.regions::Region.of(@region)
    # Parse the proxy once (and log it once) so both the async service clients and
    # the synchronous STS client used for `role_arn` assumption honor it.
    proxy_uri = extract_proxy_uri
    @aws_credentials_provider = build_credentials_provider(region, proxy_uri)
    proxy_configuration = build_proxy_configuration(proxy_uri)

    # Backward compatibility: KCL 1.x exposed `kinesis_endpoint`/`dynamodb_endpoint`
    # through additional_settings. In the AWS SDK v2 these are client-level endpoint
    # overrides, so consume them here before the remaining settings are applied.
    kinesis_endpoint = @additional_settings.delete("kinesis_endpoint")
    dynamo_db_endpoint = @additional_settings.delete("dynamodb_endpoint") || @additional_settings.delete("dynamo_db_endpoint")

    @kinesis_client = build_async_client(AWS.services.kinesis::KinesisAsyncClient, region, proxy_configuration, kinesis_endpoint)
    @dynamo_db_client = build_async_client(AWS.services.dynamodb::DynamoDbAsyncClient, region, proxy_configuration, dynamo_db_endpoint)
    @cloud_watch_client = build_async_client(AWS.services.cloudwatch::CloudWatchAsyncClient, region, proxy_configuration)

    configs_builder = KCL.common::ConfigsBuilder.new(
      @kinesis_stream_name,
      @application_name,
      @kinesis_client,
      @dynamo_db_client,
      @cloud_watch_client,
      @worker_id,
      worker_factory
    )

    # In KCL 2.x the single KinesisClientLibConfiguration is split across six
    # configuration objects. Capture each once so customizations and the
    # Scheduler reference the same instances.
    @checkpoint_config = configs_builder.checkpointConfig
    @coordinator_config = configs_builder.coordinatorConfig
    @lease_management_config = configs_builder.leaseManagementConfig
    @lifecycle_config = configs_builder.lifecycleConfig
    @metrics_config = configs_builder.metricsConfig
    @processor_config = configs_builder.processorConfig
    @retrieval_config = configs_builder.retrievalConfig

    initial_position = initial_position_in_stream_extended
    @retrieval_config.initialPositionInStreamExtended(initial_position)
    # Preserve the historical shared-throughput (polling) consumer behaviour
    # instead of the KCL 2.x default of enhanced fan-out.
    @polling_config = KCL.retrieval.polling::PollingConfig.new(@kinesis_stream_name, @kinesis_client)
    @retrieval_config.retrievalSpecificConfig(@polling_config)
    @lease_management_config.initialPositionInStream(initial_position)

    # KCL 2.x replaces the NullMetricsFactory with a metrics level; NONE disables
    # CloudWatch publishing entirely.
    if @metrics.nil?
      @metrics_config.metricsLevel(KCL.metrics::MetricsLevel::NONE)
    end

    apply_additional_settings

    @logger.info("Registered logstash-input-kinesis")
  end

  def run(output_queue)
    @output_queue = output_queue
    @kcl_scheduler = build_scheduler
    @kcl_scheduler.run
  ensure
    # make sure to close the clients when the scheduler is stopped.
    close_clients
  end

  def build_scheduler
    KCL.coordinator::Scheduler.new(
      @checkpoint_config,
      @coordinator_config,
      @lease_management_config,
      @lifecycle_config,
      @metrics_config,
      @processor_config,
      @retrieval_config
    )
  end

  def stop
    return unless @kcl_scheduler
    # Try graceful shutdown first.
    # An immediate #shutdown drops leases immediately (LEASE_LOST) without that
    # final checkpoint, which can cause the last in-flight batch to be reprocessed on
    # the next run.
    @kcl_scheduler.startGracefulShutdown
  rescue => e
    @logger.warn("Failed to start graceful Kinesis shutdown; falling back to immediate shutdown",
      :exception => e.class.to_s, :message => e.message)
    @kcl_scheduler.shutdown
  end

  def worker_factory
    proc { Worker.new(@codec.clone, @output_queue, method(:decorate), @checkpoint_interval_seconds, @logger) }
  end

  protected

  def close_clients
    [@kinesis_client, @dynamo_db_client, @cloud_watch_client].each do |client|
      begin
        client&.close
      rescue => e
        @logger.debug("Error while closing AWS client", :exception => e.class.to_s, :message => e.message)
      end
    end
  end

  def build_credentials_provider(region, proxy_uri = nil)
    base = if @profile.nil?
      AWS.auth.credentials::DefaultCredentialsProvider.create
    else
      AWS.auth.credentials::ProfileCredentialsProvider.create(@profile)
    end

    return base if @role_arn.nil?

    # Assume the role as a new layer over the credentials already created, used
    # by all of Kinesis, DynamoDB and CloudWatch. StsClient is a synchronous
    # client, so its proxy is configured on the Apache HTTP client (the async
    # Netty proxy configuration does not apply here).
    sts_builder = AWS.services.sts::StsClient.builder
      .region(region)
      .credentialsProvider(base)
    apache_proxy = build_apache_proxy_configuration(proxy_uri)
    if apache_proxy
      sts_builder.httpClientBuilder(
        AWS.http.apache::ApacheHttpClient.builder.proxyConfiguration(apache_proxy)
      )
    end
    sts_client = sts_builder.build
    assume_role_request = AWS.services.sts.model::AssumeRoleRequest.builder
      .roleArn(@role_arn)
      .roleSessionName(@role_session_name)
      .build
    AWS.services.sts.auth::StsAssumeRoleCredentialsProvider.builder
      .stsClient(sts_client)
      .refreshRequest(assume_role_request)
      .build
  end

  def build_async_client(client_class, region, proxy_configuration, endpoint = nil)
    builder = client_class.builder
      .region(region)
      .credentialsProvider(@aws_credentials_provider)
    builder.endpointOverride(java.net::URI.create(endpoint)) unless endpoint.to_s.strip.empty?
    if proxy_configuration
      builder.httpClientBuilder(
        AWS.http.nio.netty::NettyNioAsyncHttpClient.builder.proxyConfiguration(proxy_configuration)
      )
    end
    builder.build
  end

  def extract_proxy_uri
    return nil unless @http_proxy && !@http_proxy.value.to_s.strip.empty?

    proxy_uri = URI(@http_proxy.value)
    @logger.info("Using proxy #{proxy_uri.scheme}://#{proxy_uri.user}:*****@#{proxy_uri.host}:#{proxy_uri.port}")
    proxy_uri
  end

  # Netty (async) proxy configuration for the Kinesis, DynamoDB and CloudWatch clients.
  def build_proxy_configuration(proxy_uri)
    return nil if proxy_uri.nil?

    builder = AWS.http.nio.netty::ProxyConfiguration.builder
      .scheme(proxy_uri.scheme)
      .host(proxy_uri.host)
      .port(proxy_uri.port)
    builder.username(proxy_uri.user) if proxy_uri.user
    builder.password(proxy_uri.password) if proxy_uri.password
    hosts = non_proxy_hosts_set
    builder.nonProxyHosts(hosts) if hosts
    builder.build
  end

  # Apache (sync) proxy configuration for the STS client used by role assumption.
  def build_apache_proxy_configuration(proxy_uri)
    return nil if proxy_uri.nil?

    builder = AWS.http.apache::ProxyConfiguration.builder
      .endpoint(java.net::URI.create("#{proxy_uri.scheme}://#{proxy_uri.host}:#{proxy_uri.port}"))
    builder.username(proxy_uri.user) if proxy_uri.user
    builder.password(proxy_uri.password) if proxy_uri.password
    hosts = non_proxy_hosts_set
    builder.nonProxyHosts(hosts) if hosts
    builder.build
  end

  def non_proxy_hosts_set
    return nil if @non_proxy_hosts.to_s.empty?

    hosts = java.util::HashSet.new
    @non_proxy_hosts.split("|").each { |host| hosts.add(host) }
    hosts
  end

  def initial_position_in_stream_extended
    position = if @initial_position_in_stream == "LATEST"
      KCL.common::InitialPositionInStream::LATEST
    else
      KCL.common::InitialPositionInStream::TRIM_HORIZON
    end
    KCL.common::InitialPositionInStreamExtended.newInitialPosition(position)
  end

  # The KCL 2.x configuration objects that `additional_settings` groups map to.
  def additional_settings_targets
    {
      "checkpoint_config" => @checkpoint_config,
      "coordinator_config" => @coordinator_config,
      "lease_management_config" => @lease_management_config,
      "lifecycle_config" => @lifecycle_config,
      "metrics_config" => @metrics_config,
      "processor_config" => @processor_config,
      "retrieval_config" => @retrieval_config,
      "polling_config" => @polling_config,
    }
  end

  def apply_additional_settings
    targets = additional_settings_targets
    @additional_settings.each do |group, settings|
      config = targets[group]
      if config.nil?
        raise NoMethodError, "Unknown additional_settings group '#{group}'; expected one of: #{targets.keys.join(', ')}"
      end
      unless settings.is_a?(Hash)
        raise ArgumentError, "additional_settings group '#{group}' must be a hash of setting => value pairs"
      end
      settings.each do |key, value|
        unless config.respond_to?(key)
          raise NoMethodError, "Unknown additional_settings option '#{key}' for group '#{group}'"
        end
        apply_setting(config, key, value)
      end
    end
  end

  # A few KCL 2.x setters (e.g. PollingConfig#retryGetRecordsInSeconds and
  # #maxGetRecordsThreadPool) take a java.util.Optional, whereas KCL 1.x accepted
  # the bare value. Try the raw value first, then fall back to wrapping it so the
  # historical scalar `additional_settings` values keep working.
  def apply_setting(target, key, value)
    target.public_send(key, value)
  rescue NameError, TypeError, ArgumentError => original_error
    # JRuby raises NameError ("no method '...' for arguments (...)") when the setter
    # exists but no overload accepts the raw value's type, which is the case for the
    # Optional-typed KCL 2.x setters. Retry once wrapped in an Optional; if that also
    # fails the value is genuinely invalid, so surface the original error (which names
    # the setter) together with the offending key rather than the misleading Optional
    # overload error.
    begin
      target.public_send(key, java.util::Optional.ofNullable(value))
    rescue NameError, TypeError, ArgumentError
      raise original_error.class, "Invalid additional_settings value for '#{key}': #{original_error.message}"
    end
  end

  def quiet_kcl_logging
    level = org.apache.logging.log4j::Level::WARN
    ["software.amazon.kinesis", "software.amazon.awssdk"].each do |namespace|
      org.apache.logging.log4j.core.config::Configurator.setLevel(namespace, level)
    end
  rescue => e
    @logger.debug("Unable to adjust KCL log level", :exception => e.message)
  end
end
