# encoding: utf-8
require "logstash/inputs/base"
require "logstash/errors"
require "logstash/environment"
require "logstash/namespace"

require 'logstash-input-kinesis_jars'
require "logstash/inputs/kinesis/version"

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
  require "logstash/inputs/kinesis/worker"

  config_name 'kinesis'
  milestone 1

  attr_reader(
    :kcl_config,
    :kcl_builder,
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

  def initialize(params = {}, kcl_builder = KCL::Worker::Builder.new)
    @kcl_builder = kcl_builder
    super(params)
  end

  def register
    # the INFO log level is extremely noisy in KCL
    org.apache.commons.logging::LogFactory.getLog("com.amazonaws.services.kinesis").
      logger.setLevel(java.util.logging::Level::WARNING)

    worker_id = java.util::UUID.randomUUID.to_s
    creds = com.amazonaws.auth::DefaultAWSCredentialsProviderChain.new
    @kcl_config = KCL::KinesisClientLibConfiguration.new(
      @application_name,
      @kinesis_stream_name,
      creds,
      worker_id).
        withInitialPositionInStream(KCL::InitialPositionInStream::TRIM_HORIZON).
        withRegionName(@region)
  end

  def run(output_queue)
    worker_factory = proc { Worker.new(@codec.clone, output_queue, method(:decorate), @checkpoint_interval_seconds, @logger) }
    @kcl_builder.recordProcessorFactory(worker_factory)
    @kcl_builder.config(@kcl_config)

    if metrics_factory
      @kcl_builder.metricsFactory(metrics_factory)
    end

    @kcl_worker = @kcl_builder.build
    @kcl_worker.run
  end

  def teardown
    @kcl_worker.shutdown if @kcl_worker
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
