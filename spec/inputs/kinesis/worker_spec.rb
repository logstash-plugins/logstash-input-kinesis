require 'logstash-core/logstash-core'
require 'logstash-input-kinesis_jars'
require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"
require "json"

RSpec.describe "LogStash::Inputs::Kinesis::Worker" do
  EVENTS = Java::software.amazon.kinesis.lifecycle.events
  RETRIEVAL = Java::software.amazon.kinesis.retrieval

  subject!(:worker) { LogStash::Inputs::Kinesis::Worker.new(codec, queue, decorator, checkpoint_interval, logger) }
  let(:codec) { LogStash::Codecs::JSON.new() }
  let(:queue) { Queue.new }
  let(:decorator) { proc { |x| x.set('decorated', true); x } }
  let(:checkpoint_interval) { 120 }
  let(:logger) { double('logger').as_null_object }
  let(:checkpointer) { double('checkpointer', checkpoint: nil) }
  let(:init_input) { EVENTS::InitializationInput.builder.shardId("xyz").build }

  it "honors the initialize java interface method contract" do
    expect { worker.initialize(init_input) }.to_not raise_error
  end

  def record(hash, arrival_timestamp, partition_key, sequence_number)
    data = java.nio.ByteBuffer.wrap(JSON.generate(hash).to_java_bytes).asReadOnlyBuffer
    RETRIEVAL::KinesisClientRecord.builder
        .data(data)
        .approximateArrivalTimestamp(java.time.Instant.ofEpochMilli((arrival_timestamp.to_f * 1000).to_i))
        .partitionKey(partition_key)
        .sequenceNumber(sequence_number)
        .build
  end

  def process_records_input(records, checkpointer)
    EVENTS::ProcessRecordsInput.builder
        .records(java.util.Arrays.asList(records.to_java(RETRIEVAL::KinesisClientRecord)))
        .checkpointer(checkpointer)
        .build
  end

  let(:process_input) {
    process_records_input([
      record(
        { id: "record1", message: "test1" },
        '1.441215410867E9',
        'partitionKey1',
        '21269319989652663814458848515492872191'
      ),
      record(
        {
          '@metadata' => { forwarded: 'record2' },
          id: "record2",
          message: "test2"
        },
        '1.441215410868E9',
        'partitionKey2',
        '21269319989652663814458848515492872192'
      )], checkpointer)
  }
  let(:collide_metadata_process_input) {
    process_records_input([
      record(
        {
          '@metadata' => {
            forwarded: 'record3',
            partition_key: 'invalid_key'
          },
          id: "record3",
          message: "test3"
        },
        '1.441215410869E9',
        'partitionKey3',
        '21269319989652663814458848515492872193'
      )], checkpointer)
  }
  let(:empty_process_input) {
    process_records_input([], checkpointer)
  }

  context "initialized" do
    before do
      worker.initialize(init_input)
    end

    describe "#processRecords" do
      it "decodes and queues each record with decoration" do
        worker.processRecords(process_input)
        expect(queue.size).to eq(2)
        m1 = queue.pop
        m2 = queue.pop
        expect(m1).to be_kind_of(LogStash::Event)
        expect(m2).to be_kind_of(LogStash::Event)
        expect(m1.get('id')).to eq("record1")
        expect(m1.get('message')).to eq("test1")
        expect(m1.get('@metadata')['approximate_arrival_timestamp']).to eq(1441215410867)
        expect(m1.get('@metadata')['partition_key']).to eq('partitionKey1')
        expect(m1.get('@metadata')['sequence_number']).to eq('21269319989652663814458848515492872191')
        expect(m1.get('decorated')).to eq(true)
      end

      it "decodes and keeps submitted metadata" do
        worker.processRecords(process_input)
        expect(queue.size).to eq(2)
        m1 = queue.pop
        m2 = queue.pop
        expect(m1).to be_kind_of(LogStash::Event)
        expect(m2).to be_kind_of(LogStash::Event)
        expect(m1.get('@metadata')['forwarded']).to eq(nil)
        expect(m2.get('@metadata')['forwarded']).to eq('record2')
      end

      it "decodes and does not allow submitted metadata to overwrite internal keys" do
        worker.processRecords(collide_metadata_process_input)
        expect(queue.size).to eq(1)
        m1 = queue.pop
        expect(m1).to be_kind_of(LogStash::Event)
        expect(m1.get('@metadata')['forwarded']).to eq('record3')
        expect(m1.get('@metadata')['partition_key']).to eq('partitionKey3')
      end

      it "checkpoints on interval" do
        expect(checkpointer).to receive(:checkpoint).once
        worker.processRecords(empty_process_input)

        # not this time
        worker.processRecords(empty_process_input)

        allow(Time).to receive(:now).and_return(Time.now + 125)
        expect(checkpointer).to receive(:checkpoint).once
        worker.processRecords(empty_process_input)
      end
    end

    describe "#shardEnded" do
      it "checkpoints when the shard ends" do
        expect(checkpointer).to receive(:checkpoint)
        input = EVENTS::ShardEndedInput.builder.checkpointer(checkpointer).build
        worker.shardEnded(input)
      end
    end

    describe "#shutdownRequested" do
      it "checkpoints on shutdown" do
        expect(checkpointer).to receive(:checkpoint)
        input = EVENTS::ShutdownRequestedInput.builder.checkpointer(checkpointer).build
        worker.shutdownRequested(input)
      end
    end
  end
end
