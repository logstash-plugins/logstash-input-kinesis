# encoding: utf-8
class LogStash::Inputs::Kinesis::Worker
  include Java::software.amazon.kinesis.processor::ShardRecordProcessor

  attr_reader(
    :checkpoint_interval,
    :codec,
    :decorator,
    :logger,
    :output_queue,
  )

  def initialize(*args)
    # nasty hack, because this is the name of a method on ShardRecordProcessor, but also ruby's constructor
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
    # Required by the ShardRecordProcessor interface. Intentionally a no-op:
    # the lease (and shard) now belongs to another worker, and KCL provides no
    # checkpointer on LeaseLostInput, so there is nothing to do here.
  end

  def shardEnded(shard_ended_input)
    checkpoint(shard_ended_input.checkpointer)
  end

  def shutdownRequested(shutdown_requested_input)
    checkpoint(shutdown_requested_input.checkpointer)
  end

  protected

  def checkpoint(checkpointer)
    checkpointer.checkpoint()
  rescue => error
    @logger.error("Kinesis worker failed checkpointing: #{error}")
  end

  def process_record(record)
    buffer = record.data.duplicate
    bytes = Java::byte[buffer.remaining].new
    buffer.get(bytes)
    raw = String.from_java_bytes(bytes)
    metadata = build_metadata(record)
    @codec.decode(raw) do |event|
      @decorator.call(event)
      event.set('@metadata', event.get('@metadata').merge(metadata))
      @output_queue << event
    end
  rescue => error
    @logger.error("Error processing record: #{error}", :exception => error.class.to_s, :backtrace => error.backtrace)
  end

  def build_metadata(record)
    metadata = Hash.new
    metadata['approximate_arrival_timestamp'] = record.approximate_arrival_timestamp.to_epoch_milli
    metadata['partition_key'] = record.partition_key
    metadata['sequence_number'] = record.sequence_number
    metadata
  end

end
