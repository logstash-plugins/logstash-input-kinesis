# encoding: utf-8

require "socket"
require "logstash/inputs/base"
require "logstash/errors"
require "logstash/environment"
require "logstash/namespace"

require 'logstash-input-kinesis_jars'


# Receive events through an AWS Kinesis stream.
#
# This input plugin uses the Java Kinesis Client Library underneath, so the
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
  KCL = com.amazonaws.services.kinesis.clientlibrary.lib.worker
  KCL_PROCESSOR_FACTORY_CLASS = com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
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

  # Any additional arbitrary kcl options configurable in the KinesisClientLibConfiguration
  config :additional_settings, :validate => :hash, :default => {}

  def initialize(params = {})
    super(params)
  end

  def register
    # the INFO log level is extremely noisy in KCL
    lg = org.apache.commons.logging::LogFactory.getLog("com.amazonaws.services.kinesis")
    if lg.java_kind_of?(org.apache.commons.logging.impl::Jdk14Logger)
      kinesis_logger = lg.logger
      if kinesis_logger.java_kind_of?(java.util.logging::Logger)
        kinesis_logger.setLevel(java.util.logging::Level::WARNING)
      else
        kinesis_logger.setLevel(org.apache.log4j::Level::WARN)
      end
    elsif lg.java_kind_of?(org.apache.logging.log4jJcl::Log4jLog)
      logContext = org.apache.logging.log4j::LogManager.getContext(false)
      config = logContext.getConfiguration()
      config.getLoggerConfig("com.amazonaws.services.kinesis").setLevel(org.apache.logging.log4j::Level::WARN)
    else
      raise "Can't configure WARN log level for logger wrapper class #{lg.class}"
    end

    hostname = Socket.gethostname
    uuid = java.util::UUID.randomUUID.to_s
    worker_id = "#{hostname}:#{uuid}"

    # If the AWS profile is set, use the profile credentials provider.
    # Otherwise fall back to the default chain.
    unless @profile.nil?
      creds = com.amazonaws.auth.profile::ProfileCredentialsProvider.new(@profile)
    else
      creds = com.amazonaws.auth::DefaultAWSCredentialsProviderChain.new
    end

    # If a role ARN is set then assume the role as a new layer over the credentials already created
    unless @role_arn.nil?
      kinesis_creds = com.amazonaws.auth::STSAssumeRoleSessionCredentialsProvider.new(creds, @role_arn, @role_session_name)
    else
      kinesis_creds = creds
    end

    initial_position_in_stream = if @initial_position_in_stream == "TRIM_HORIZON"
      KCL::InitialPositionInStream::TRIM_HORIZON
    else
      KCL::InitialPositionInStream::LATEST
    end

    @kcl_config = KCL::KinesisClientLibConfiguration.new(
      @application_name,
      @kinesis_stream_name,
      kinesis_creds, # credential provider for Kinesis, DynamoDB and Cloudwatch access
      worker_id).
        withInitialPositionInStream(initial_position_in_stream).
        withRegionName(@region)

      # Call arbitrary "withX()" functions
      # snake_case => withCamelCase happens automatically
      @additional_settings.each do |key, value|
          fn = "with_#{key}"
          @kcl_config.send(fn, value)
      end
  end

  def run(output_queue)
    @kcl_worker = kcl_builder(output_queue).build
    @kcl_worker.run
  end

  def kcl_builder(output_queue)
    KCL::Worker::Builder.new.tap do |builder|
      builder.java_send(:recordProcessorFactory, [KCL_PROCESSOR_FACTORY_CLASS.java_class], worker_factory(output_queue))
      builder.config(@kcl_config)

      if metrics_factory
        builder.metricsFactory(metrics_factory)
      end
    end
  end

  def stop
    @kcl_worker.shutdown if @kcl_worker
  end

  def worker_factory(output_queue)
    proc { Worker.new(@codec.clone, output_queue, method(:decorate), @checkpoint_interval_seconds, @logger) }
  end

  protected

  def metrics_factory
    case @metrics
    when nil
      com.amazonaws.services.kinesis.metrics.impl::NullMetricsFactory.new
    when 'cloudwatch'
      nil # default in the underlying library
    end
  end
end
