require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"


RSpec.describe "inputs/kinesis" do
  KCL = com.amazonaws.services.kinesis.clientlibrary.lib.worker

  # Since kinesis.rb is using now a builder, subclassing the kinesis Worker is no longer needed.
  # This test only need to check if run method is called on the instance returned
  class TestKCLBuilder < KCL::Worker::Builder
    attr_reader :kcl_worker_factory, :kcl_metrics_factory

    def initialize(worker_double)
      super()
      @worker = worker_double
    end

    # mimics the actual interface
    def metricsFactory(factory)
      @kcl_metrics_factory = factory
    end     

    # mimics the actual interface
    def recordProcessorFactory(factory)
      @kcl_worker_factory = factory
    end 

    def build
      return @worker
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

  subject!(:kinesis) { LogStash::Inputs::Kinesis.new(config, kcl_builder) }
  let(:kcl_worker) { double('kcl_worker', run: nil) }
  let(:kcl_builder) { TestKCLBuilder.new(kcl_worker) }
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

    it "runs the KCL worker" do
      expect(kcl_worker).to receive(:run).with(no_args)
      kinesis.run(queue)
    end

    it "clones the codec for each worker" do
      expect(codec).to receive(:clone).once
      kinesis.run(queue)
      
      worker = kinesis.kcl_builder.kcl_worker_factory.call()
      expect(worker).to be_kind_of(LogStash::Inputs::Kinesis::Worker)
    end

    it "generates a valid worker via the factory proc" do
      kinesis.run(queue)
      worker = kinesis.kcl_builder.kcl_worker_factory.call()
      
      expect(worker.codec).to be_kind_of(codec.class)
      expect(worker.checkpoint_interval).to eq(120)
      expect(worker.output_queue).to eq(queue)
      expect(worker.decorator).to eq(kinesis.method(:decorate))
      expect(worker.logger).to eq(kinesis.logger)
    end

    it "disables metric tracking by default" do
      kinesis.run(queue)
      expect(kinesis.kcl_builder.kcl_metrics_factory).to be_kind_of(com.amazonaws.services.kinesis.metrics.impl::NullMetricsFactory)
    end

    context "cloudwatch" do
      let(:metrics) { "cloudwatch" }
      it "uses cloudwatch metrics if specified" do
        kinesis.run(queue)
        # since the behaviour is enclosed on private methods it is not testable. So here 
        # the expected value can be tested, not the result associated to set this value
        expect(kinesis.kcl_builder.kcl_metrics_factory).to eq(nil)
      end
    end
  end
end
