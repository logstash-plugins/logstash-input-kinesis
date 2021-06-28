# encoding: utf-8
class LogStash::Inputs::Kinesis::Worker
  include com.amazonaws.services.kinesis.clientlibrary.interfaces.v2::IRecordProcessor

  attr_reader(
    :checkpoint_interval,
    :codec,
    :decorator,
    :logger,
    :output_queue,
  )

  def initialize(*args)
    # nasty hack, because this is the name of a method on IRecordProcessor, but also ruby's constructor
    if !@constructed
      @codec, @output_queue, @decorator, @checkpoint_interval, @logger = args
      @next_checkpoint = Time.now - 600
      @constructed = true
    else
      _shard_id = args[0].shardId
    end
  end
  public :initialize

  def processRecords(records_input)
    records_input.records.each { |record| process_record(record) }
    if Time.now >= @next_checkpoint
      checkpoint(records_input.checkpointer)
      @next_checkpoint = Time.now + @checkpoint_interval
    end
  end

  def shutdown(shutdown_input)
    if shutdown_input.shutdown_reason == com.amazonaws.services.kinesis.clientlibrary.lib.worker::ShutdownReason::TERMINATE
      checkpoint(shutdown_input.checkpointer)
    end
  end

  protected

  def checkpoint(checkpointer)
    checkpointer.checkpoint()
  rescue => error
    @logger.error("Kinesis worker failed checkpointing: #{error}")
  end

  def process_record(record)
    raw = String.from_java_bytes(record.getData.array)
    metadata = build_metadata(record)
    @codec.decode(raw) do |event|
      @decorator.call(event)
      event.set('@metadata', event.get('@metadata').merge(metadata))
      @output_queue << event
    end
  rescue => error
    @logger.error("Error processing record: #{error}")
  end

  def build_metadata(record)
    metadata = Hash.new
    metadata['approximate_arrival_timestamp'] = record.getApproximateArrivalTimestamp.getTime
    metadata['partition_key'] = record.getPartitionKey
    metadata['sequence_number'] = record.getSequenceNumber
    metadata
  end

end
