require 'logstash-core/logstash-core'
require 'logstash-input-kinesis_jars'
require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"
require "json"

RSpec.describe "LogStash::Inputs::Kinesis::Worker" do
  KCL_TYPES = com.amazonaws.services.kinesis.clientlibrary.types

  subject!(:worker) { LogStash::Inputs::Kinesis::Worker.new(codec, queue, decorator, checkpoint_interval) }
  let(:codec) { LogStash::Codecs::JSON.new() }
  let(:queue) { Queue.new }
  let(:decorator) { proc { |x| x.set('decorated', true); x } }
  let(:checkpoint_interval) { 120 }
  let(:checkpointer) { double('checkpointer', checkpoint: nil) }
  let(:init_input) { KCL_TYPES::InitializationInput.new().withShardId("xyz") }

  it "honors the initialize java interface method contract" do
    expect { worker.initialize(init_input) }.to_not raise_error
  end

  def record(hash = { "message" => "test" }, arrival_timestamp, partition_key, sequence_number)
    encoder = java.nio.charset::Charset.forName("UTF-8").newEncoder()
    data = encoder.encode(java.nio.CharBuffer.wrap(JSON.generate(hash)))
    double(
      getData: data,
      getApproximateArrivalTimestamp: java.util.Date.new(arrival_timestamp.to_f * 1000),
      getPartitionKey: partition_key,
      getSequenceNumber: sequence_number
    )
  end

  let(:process_input) {
    KCL_TYPES::ProcessRecordsInput.new()
        .withRecords(java.util.Arrays.asList([
          record(
            {
              id: "record1",
              message: "test1"
            },
            '1.441215410867E9',
            'partitionKey1',
            '21269319989652663814458848515492872191'
          ),
          record(
            {
              '@metadata': {
                forwarded: 'record2'
              },
              id: "record2",
              message: "test2"
            },
            '1.441215410868E9',
            'partitionKey2',
            '21269319989652663814458848515492872192'
          )].to_java)
        )
        .withCheckpointer(checkpointer)
  }
  let(:collide_metadata_process_input) {
    KCL_TYPES::ProcessRecordsInput.new()
        .withRecords(java.util.Arrays.asList([
          record(
            {
              '@metadata': {
                forwarded: 'record3',
                partition_key: 'invalid_key'
              },
            id: "record3",
            message: "test3"
            },
            '1.441215410869E9',
            'partitionKey3',
            '21269319989652663814458848515492872193'
          )].to_java)
        )
        .withCheckpointer(checkpointer)
  }
  let(:empty_process_input) {
    KCL_TYPES::ProcessRecordsInput.new()
        .withRecords(java.util.Arrays.asList([].to_java))
        .withCheckpointer(checkpointer)
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

    describe "#shutdown" do
      it "checkpoints on termination" do
        input = KCL_TYPES::ShutdownInput.new
        checkpointer = double('checkpointer')
        expect(checkpointer).to receive(:checkpoint)
        input.
          with_shutdown_reason(com.amazonaws.services.kinesis.clientlibrary.lib.worker::ShutdownReason::TERMINATE).
          with_checkpointer(checkpointer)
        worker.shutdown(input)
      end
    end
  end
end
