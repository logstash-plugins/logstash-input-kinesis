require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"

RSpec.describe "inputs/kinesis" do
  KCL = com.amazonaws.services.kinesis.clientlibrary.lib.worker

  # It's very difficult to directly test the Java class, with all its private members,
  # so we subclass it here.
  class TestKCLWorker < KCL::Worker
    field_reader :metricsFactory, :recordProcessorFactory
    def run
    end
  end

  let(:config) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
  }}

  subject!(:kinesis) { LogStash::Inputs::Kinesis.new(config, kcl_class) }
  let(:kcl_class) { TestKCLWorker }
  let(:metrics) { nil }
  let(:codec) { LogStash::Codecs::JSON.new() }

  it "registers without error" do
    input = LogStash::Plugin.lookup("input", "kinesis").new("kinesis_stream_name" => "specs", "codec" => codec)
    expect { input.register }.to_not raise_error
  end

  it "configures the KCL" do
    kinesis.register
    expect(kinesis.kcl_config.applicationName).to eq("my-processor")
    expect(kinesis.kcl_config.streamName).to eq("run-specs")
    expect(kinesis.kcl_config.regionName).to eq("ap-southeast-1")
    expect(kinesis.kcl_config.initialPositionInStream).to eq(KCL::InitialPositionInStream::TRIM_HORIZON)
  end

  context "#run" do
    let(:queue) { Queue.new }

    before do
      kinesis.register
    end

    it "clones the codec for each worker" do
      expect(codec).to receive(:clone).once
      kinesis.run(queue)
      worker = kinesis.kcl_worker.recordProcessorFactory.call()
      expect(worker).to be_kind_of(LogStash::Inputs::Kinesis::Worker)
    end

    it "generates a valid worker via the factory proc" do
      kinesis.run(queue)
      worker = kinesis.kcl_worker.recordProcessorFactory.call()
      expect(worker.codec).to be_kind_of(codec.class)
      expect(worker.checkpoint_interval).to eq(120)
      expect(worker.output_queue).to eq(queue)
      expect(worker.decorator).to eq(kinesis.method(:decorate))
      expect(worker.logger).to eq(kinesis.logger)
    end

    it "disables metric tracking by default" do
      kinesis.run(queue)
      expect(kinesis.kcl_worker.metricsFactory).to be_kind_of(com.amazonaws.services.kinesis.metrics.impl::NullMetricsFactory)
    end

    context "cloudwatch" do
      let(:metrics) { "cloudwatch" }
      it "uses cloudwatch metrics if specified" do
        kinesis.run(queue)
        expect(kinesis.kcl_worker.metricsFactory).to be_kind_of(com.amazonaws.services.kinesis.metrics.impl::CWMetricsFactory)
        # the process hangs otherwise
        kinesis.kcl_worker.metricsFactory.shutdown
      end
    end
  end
end
