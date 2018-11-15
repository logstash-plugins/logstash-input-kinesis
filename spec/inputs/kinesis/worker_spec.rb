require 'logstash-core/logstash-core'
require 'logstash-input-kinesis_jars'
require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"
require "json"

RSpec.describe "LogStash::Inputs::Kinesis::Worker" do
  software = Java::Software
  KCL_TYPES = software.amazon.kinesis.lifecycle

  subject!(:worker) { LogStash::Inputs::Kinesis::Worker.new(codec, queue, decorator, checkpoint_interval, org.apache.logging.log4j.LogManager.getLogger()) }
  let(:codec) { LogStash::Codecs::JSON.new() }
  let(:queue) { Queue.new }
  let(:decorator) { proc { |x| x.set('decorated', true); x } }
  let(:checkpoint_interval) { 120 }
  let(:checkpointer) { double('checkpointer', checkpoint: nil) }
  let(:init_input) { KCL_TYPES.events::InitializationInput.builder().shardId("xyz").build }

  it "honors the initialize java interface method contract" do
    expect { worker.initialize(init_input) }.to_not raise_error
  end

  def record(hash = { "message" => "test" }, arrival_timestamp, partition_key, sequence_number)
    encoder = java.nio.charset::Charset.forName("UTF-8").newEncoder()
    data = encoder.encode(java.nio.CharBuffer.wrap(JSON.generate(hash)))
    double(
      data: data,
      approximateArrivalTimestamp: java.time.Instant.ofEpochMilli(arrival_timestamp.to_f * 1000),
      partitionKey: partition_key,
      sequenceNumber: sequence_number
    )
  end

  let(:process_input) {
    KCL_TYPES.events::ProcessRecordsInput.builder()
        .records(java.util.Arrays.asList([
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
              id: "record2",
              message: "test2"
            },
            '1.441215410868E9',
            'partitionKey2',
            '21269319989652663814458848515492872192'
          )].to_java)
        )
        .checkpointer(checkpointer)
        .build
  }
  let(:empty_process_input) {
    KCL_TYPES.events::ProcessRecordsInput.builder()
        .records(java.util.Arrays.asList([].to_java))
        .checkpointer(checkpointer)
        .build
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
        checkpointer = double('checkpointer')
        expect(checkpointer).to receive(:checkpoint)
        worker.shutdownRequested(KCL_TYPES::ShutdownInput.builder()
          .shutdownReason(KCL_TYPES::ShutdownReason::REQUESTED)
          .checkpointer(checkpointer)
          .build)
      end

      it "checkpoints on shard ended" do
        checkpointer = double('checkpointer')
        expect(checkpointer).to receive(:checkpoint)
        worker.shardEnded(KCL_TYPES::ShutdownInput.builder()
          .shutdownReason(KCL_TYPES::ShutdownReason::SHARD_END)
          .checkpointer(checkpointer)
          .build)
      end
    end
  end
end
