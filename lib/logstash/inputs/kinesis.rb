# encoding: utf-8
require "logstash/inputs/base"
require "logstash/errors"
require "logstash/environment"
require "logstash/namespace"

require "logstash/inputs/kinesis/version"
require 'logstash-input-kinesis_jars'

class LogStash::Inputs::Kinesis < LogStash::Inputs::Base
  KCL = com.amazonaws.services.kinesis.clientlibrary.lib.worker

  config_name 'kinesis'
  milestone 1

  # The application name used for the dynamodb coordination table. Must be
  # unique for this kinesis stream.
  config :application_name, :validate => :string, :default => "logstash"

  # The kinesis stream name.
  config :kinesis_stream_name, :validate => :string, :required => true

  # How many seconds between worker checkpoints to dynamodb.
  config :checkpoint_interval_seconds, :validate => :number, :default => 60

  def register
    # the INFO log level is extremely noisy in KCL
    org.apache.commons.logging::LogFactory.getLog("com.amazonaws.services.kinesis").
      logger.setLevel(java.util.logging::Level::WARNING)

    worker_id = java.util::UUID.randomUUID.to_s
    creds = com.amazonaws.auth::DefaultAWSCredentialsProviderChain.new()
    @config = KCL::KinesisClientLibConfiguration.new(
      @application_name,
      @kinesis_stream_name,
      creds,
      worker_id).withInitialPositionInStream(KCL::InitialPositionInStream::TRIM_HORIZON)
  end

  def run(output_queue)
    @worker = KCL::Worker.new(
      proc { Worker.new(@codec, output_queue, method(:decorate), @checkpoint_interval_seconds) },
      @config,
      com.amazonaws.services.kinesis.metrics.impl::NullMetricsFactory.new)
    @worker.run()
  end

  def teardown
    @worker.shutdown if @worker
  end

  class Worker
    include com.amazonaws.services.kinesis.clientlibrary.interfaces::IRecordProcessor

    def initialize(*args)
      # nasty hack, because this is the name of a method on IRecordProcessor, but also ruby's constructor
      if !@constructed
        @codec, @output_queue, @decorator, @checkpoint_interval = args
        @next_checkpoint = Time.now - 600
        @constructed = true
      else
        _shard_id, _ = args
        @decoder = java.nio.charset::Charset.forName("UTF-8").newDecoder()
      end
    end

    def processRecords(records, checkpointer)
      records.each { |record| process_record(record) }
      if Time.now >= @next_checkpoint
        checkpoint(checkpointer)
        @next_checkpoint = Time.now + @checkpoint_interval
      end
    end

    def shutdown(checkpointer, reason)
      if reason == com.amazonaws.services.kinesis.clientlibrary.types::ShutdownReason::TERMINATE
        checkpoint(checkpointer)
      end
    end

    protected

    def checkpoint(checkpointer)
      checkpointer.checkpoint()
    rescue => error
      @logger.error("Kinesis worker failed checkpointing: #{error}")
    end

    def process_record(record)
      raw = @decoder.decode(record.getData).to_s
      @codec.decode(raw) do |event|
        @decorator.call(event)
        @output_queue << event
      end
    rescue => error
      @logger.error("Error processing record: #{error}")
    end
  end
end
