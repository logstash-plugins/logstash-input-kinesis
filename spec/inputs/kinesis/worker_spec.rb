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
  let(:decorator) { proc { |x| x["decorated"] = true; x } }
  let(:checkpoint_interval) { 120 }
  let(:checkpointer) { double('checkpointer', checkpoint: nil) }
  let(:init_input) { KCL_TYPES::InitializationInput.new().withShardId("xyz") }

  it "honors the initialize java interface method contract" do
    expect { worker.initialize(init_input) }.to_not raise_error
  end

  def record(hash = { "message" => "test" })
    encoder = java.nio.charset::Charset.forName("UTF-8").newEncoder()
    data = encoder.encode(java.nio.CharBuffer.wrap(JSON.generate(hash)))
    double(getData: data)
  end

  let(:process_input) {
    KCL_TYPES::ProcessRecordsInput.new()
        .withRecords(java.util.Arrays.asList([
            record(id: "record1", message: "test1"),
            record(id: "record2", message: "test2")
          ].to_java)
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
        m1 = queue.pop
        m2 = queue.pop
        expect(m1).to be_kind_of(LogStash::Event)
        expect(m2).to be_kind_of(LogStash::Event)
        expect(m1['id']).to eq("record1")
        expect(m1['message']).to eq("test1")
        expect(m1['decorated']).to eq(true)
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
          with_shutdown_reason(KCL_TYPES::ShutdownReason::TERMINATE).
          with_checkpointer(checkpointer)
        worker.shutdown(input)
      end
    end
  end
end
