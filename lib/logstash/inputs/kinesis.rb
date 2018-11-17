# encoding: utf-8

require "socket"
require "logstash/inputs/base"
require "logstash/errors"
require "logstash/environment"
require "logstash/namespace"

require 'logstash-input-kinesis_jars'
require "logstash/inputs/kinesis/version"


def software
  Java::Software
end

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
  ConfigsBuilder = software.amazon.kinesis.common.ConfigsBuilder
  ProcessorConfig = software.amazon.kinesis.processor.ProcessorConfig
  Scheduler = software.amazon.kinesis.coordinator.Scheduler

  KinesisClientUtil = software.amazon.kinesis.common.KinesisClientUtil
  KinesisAsyncClient = software.amazon.awssdk.services.kinesis.KinesisAsyncClient
  DynamoDbAsyncClient = software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
  CloudWatchAsyncClient = software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient

  require "logstash/inputs/kinesis/worker"

  config_name 'kinesis'

  attr_reader(
    :kcl_config,
    :kcl_worker,

    :checkpoint_config,
    :coordinator_config,
    :lease_management_config,
    :lifecycle_config,
    :metrics_config,
    :retrieval_config,
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

  # Select initial_position_in_stream. Accepts TRIM_HORIZON or LATEST
  config :initial_position_in_stream, :validate => ["TRIM_HORIZON", "LATEST"], :default => "TRIM_HORIZON"

  # Any additional arbitrary kcl options configurable in the CheckpointConfig
  config :checkpoint_additional_settings, :validate => :hash, :default => {}

  # Any additional arbitrary kcl options configurable in the CoordinatorConfig
  config :coordinator_additional_settings, :validate => :hash, :default => {}

  # Any additional arbitrary kcl options configurable in the LeaseManagementConfig
  config :lease_management_additional_settings, :validate => :hash, :default => {}

  # Any additional arbitrary kcl options configurable in the LifecycleConfig
  config :lifecycle_additional_settings, :validate => :hash, :default => {}

  # Any additional arbitrary kcl options configurable in the MetricsConfig
  config :metrics_additional_settings, :validate => :hash, :default => {}

  # Any additional arbitrary kcl options configurable in the RetrievalConfig
  config :retrieval_additional_settings, :validate => :hash, :default => {}

  # Any additional arbitrary kcl options configurable in the ProcessorConfig
  config :processor_additional_settings, :validate => :hash, :default => {}

  def initialize(params = {})
    super(params)
  end

  def register
    hostname = Socket.gethostname
    uuid = java.util::UUID.randomUUID.to_s
    worker_id = "#{hostname}:#{uuid}"

    # If the AWS profile is set, use the profile credentials provider.
    # Otherwise fall back to the default chain.
    unless @profile.nil?
      creds = software.amazon.awssdk.auth.credentials::ProfileCredentialsProvider.create(@profile)
    else
      creds = software.amazon.awssdk.auth.credentials::DefaultCredentialsProvider.create
    end
    initial_position_in_stream = if @initial_position_in_stream == "TRIM_HORIZON"
      software.amazon.kinesis.common.InitialPositionInStream::TRIM_HORIZON
    else
      software.amazon.kinesis.common.InitialPositionInStream::LATEST
    end

    region = software.amazon.awssdk.regions.Region.of(@region)

    kinesis_client = KinesisClientUtil.create_kinesis_async_client(KinesisAsyncClient.builder().region(region).credentials_provider(creds))
    dynamodb_client = DynamoDbAsyncClient.builder().region(region).credentials_provider(creds).build
    cloudwatch_client = CloudWatchAsyncClient.builder().region(region).credentials_provider(creds).build

    @kcl_config = ConfigsBuilder.new(
      @kinesis_stream_name,
      @application_name,
      kinesis_client,
      dynamodb_client,
      cloudwatch_client,
      worker_id,
      # cannot pass nil into ConfigsBuilder because Lombok will throw an NPE - this value
      # (processor_config) should not be used.
      worker_factory([])
    )

    @checkpoint_config = send_additional_settings(@kcl_config.checkpoint_config, @checkpoint_additional_settings)

    @coordinator_config = send_additional_settings(@kcl_config.coordinator_config, @coordinator_additional_settings)

    @lifecycle_config = send_additional_settings(@kcl_config.lifecycle_config, @lifecycle_additional_settings)

    @metrics_config = @kcl_config.metrics_config.metrics_factory(metrics_factory)
    @metrics_config = send_additional_settings(@metrics_config, @metrics_additional_settings)

    @retrieval_config = @kcl_config.retrieval_config.
      initial_position_in_stream_extended(software.amazon.kinesis.common.InitialPositionInStreamExtended.new_initial_position(initial_position_in_stream))
    @retrieval_config = send_additional_settings(@retrieval_config, @retrieval_additional_settings)

    @lease_management_config = @kcl_config.lease_management_config.
      failover_time_millis(@checkpoint_interval_seconds * 1000 * 3)
    @lease_management_config = send_additional_settings(@lease_management_config, @lease_management_additional_settings)
  end

  def send_additional_settings(obj, options)
    options.each do |key, value|
      obj = obj.send(key, value)
    end

    obj
  end

  def run(output_queue)
    @kcl_worker = kcl_builder(output_queue)
    @kcl_worker.run
  end

  def kcl_builder(output_queue)
    Scheduler.new(
      @checkpoint_config,
      @coordinator_config,
      @lease_management_config,
      @lifecycle_config,
      @metrics_config,
      send_additional_settings(ProcessorConfig.new(worker_factory(output_queue)), @processor_additional_settings),
      @retrieval_config
    )
  end

  def stop
    if not @kcl_worker
      return
    end

    graceful_shutdown_future = @kcl_worker.start_graceful_shutdown

    begin
      graceful_shutdown_future.get(20, java.util.concurrent.TimeUnit::SECONDS)
    rescue java.lang.InterruptedException
      @logger.info("Interrupted while waiting for graceful shutdown. Attempting to force shutdown.")
      @kcl_worker.shutdown
    rescue java.util.concurrent.ExecutionException => error
      @logger.error("Exception while executing graceful shutdown.", error)
      raise error
    rescue java.util.concurrent.TimeoutException
      @logger.error("Timeout while waiting for shutdown. Scheduler may not have exited. Attempting to force shutdown.")
      @kcl_worker.shutdown
    end
  end

  def worker_factory(output_queue)
    proc { Worker.new(@codec.clone, output_queue, method(:decorate), @checkpoint_interval_seconds, @logger) }
  end

  protected

  def metrics_factory
    case @metrics
    when nil
      software.amazon.kinesis.metrics::NullMetricsFactory.new
    when 'cloudwatch'
      nil # default in the underlying library
    end
  end
end
