# encoding: utf-8
class LogStash::Inputs::Kinesis::Worker
  software = Java::Software

  include software.amazon.kinesis.processor::ShardRecordProcessor

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

  def leaseLost(lease_lost_input)
  end

  def shardEnded(shard_ended_input)
    checkpoint(shard_ended_input.checkpointer)
  end

  def shutdownRequested(shutdown_input)
    checkpoint(shutdown_input.checkpointer)
  end

  protected

  def checkpoint(checkpointer)
    checkpointer.checkpoint()
  rescue => error
    @logger.error("Kinesis worker failed checkpointing: #{error}", error)
  end

  def process_record(record)
    buf = record.data
    buf.rewind
    bytes = Java::byte[record.data.remaining].new
    buf.get(bytes)

    raw = String.from_java_bytes(bytes)

    metadata = build_metadata(record)
    @codec.decode(raw) do |event|
      @decorator.call(event)
      event.set('@metadata', metadata)
      @output_queue << event
    end
  rescue => error
    @logger.error("Error processing record: #{error}", error)
  end

  def build_metadata(record)
    metadata = Hash.new
    metadata['approximate_arrival_timestamp'] = record.approximateArrivalTimestamp.toEpochMilli
    metadata['partition_key'] = record.partitionKey
    metadata['sequence_number'] = record.sequenceNumber
    metadata
  end

end
