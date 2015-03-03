require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"
require "json"

RSpec.describe "LogStash::Inputs::Kinesis::Worker" do
  subject!(:worker) { LogStash::Inputs::Kinesis::Worker.new(codec, queue, decorator, checkpoint_interval) }
  let(:codec) { LogStash::Codecs::JSON.new() }
  let(:queue) { Queue.new }
  let(:decorator) { proc { |x| x["decorated"] = true; x } }
  let(:checkpoint_interval) { 120 }
  let(:checkpointer) { double('checkpointer', checkpoint: nil) }
  let(:shard_id) { "xyz" }

  it "honors the initialize java interface method contract" do
    expect { worker.initialize(shard_id) }.to_not raise_error
  end

  def record(hash = { "message" => "test" })
    encoder = java.nio.charset::Charset.forName("UTF-8").newEncoder()
    data = encoder.encode(java.nio.CharBuffer.wrap(JSON.generate(hash)))
    double(getData: data)
  end

  let(:record1) { record(id: "record1", message: "test1") }
  let(:record2) { record(id: "record2", message: "test2") }

  context "initialized" do
    before do
      worker.initialize(shard_id)
    end

    describe "#processRecords" do
      it "decodes and queues each record with decoration" do
        worker.processRecords([record1, record2], checkpointer)
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
        worker.processRecords([], checkpointer)

        # not this time
        worker.processRecords([], checkpointer)

        allow(Time).to receive(:now).and_return(Time.now + 125)
        expect(checkpointer).to receive(:checkpoint).once
        worker.processRecords([], checkpointer)
      end
    end
  end
end
