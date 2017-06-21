require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"

KCL2 = com.amazonaws.services.kinesis.clientlibrary.lib.worker

config2={
  "application_name" => "my-processor",
  "kinesis_stream_name" => "run-specs",
  "codec" => LogStash::Codecs::JSON.new(),
  "metrics" => nil,
  "checkpoint_interval_seconds" => 120,
  "region" => "ap-southeast-1",
}

kinesis2=LogStash::Inputs::Kinesis.new(config2)
kinesis2.register

puts kinesis2.kcl_config.get_kinesis_credentials_provider.getClass.to_s
if kinesis2.kcl_config.get_kinesis_credentials_provider.getClass.to_s == "com.amazonaws.auth.profile.ProfileCredentialsProvider"
  puts "tes"
else
  puts "nes"
end

RSpec.describe "inputs/kinesis" do
  KCL = com.amazonaws.services.kinesis.clientlibrary.lib.worker

  let(:config) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil
  }}

  # Config hash to test credentials provider to be used if profile is specified
  let(:config_with_profile) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => "my-aws-profile"
  }}

  subject!(:kinesis) { LogStash::Inputs::Kinesis.new(config) }
  let(:kcl_worker) { double('kcl_worker') }
  let(:stub_builder) { double('kcl_builder', build: kcl_worker) }
  let(:metrics) { nil }
  let(:codec) { LogStash::Codecs::JSON.new() }
  let(:queue) { Queue.new }

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
    expect(kinesis.kcl_config.get_kinesis_credentials_provider.getClass.to_s).to eq("com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
  end

  subject!(:kinesis_with_profile) { LogStash::Inputs::Kinesis.new(config_with_profile) }

  it "uses ProfileCredentialsProvider if profile is specified" do
    kinesis_with_profile.register
    expect(kinesis_with_profile.kcl_config.get_kinesis_credentials_provider.getClass.to_s).to eq("com.amazonaws.auth.profile.ProfileCredentialsProvider")
  end

  context "#run" do
    it "runs the KCL worker" do
      expect(kinesis).to receive(:kcl_builder).with(queue).and_return(stub_builder)
      expect(kcl_worker).to receive(:run).with(no_args)
      builder = kinesis.run(queue)
    end
  end

  context "#stop" do
    it "stops the KCL worker" do
      expect(kinesis).to receive(:kcl_builder).with(queue).and_return(stub_builder)
      expect(kcl_worker).to receive(:run).with(no_args)
      expect(kcl_worker).to receive(:shutdown).with(no_args)
      kinesis.run(queue)
      kinesis.do_stop # do_stop calls stop internally
    end
  end

  context "#worker_factory" do
    it "clones the codec for each worker" do
      worker = kinesis.worker_factory(queue).call()
      expect(worker).to be_kind_of(LogStash::Inputs::Kinesis::Worker)
      expect(worker.codec).to_not eq(kinesis.codec)
      expect(worker.codec).to be_kind_of(codec.class)
    end

    it "generates a valid worker" do
      worker = kinesis.worker_factory(queue).call()

      expect(worker.codec).to be_kind_of(codec.class)
      expect(worker.checkpoint_interval).to eq(120)
      expect(worker.output_queue).to eq(queue)
      expect(worker.decorator).to eq(kinesis.method(:decorate))
      expect(worker.logger).to eq(kinesis.logger)
    end
  end

  # these tests are heavily dependent on the current Worker::Builder
  # implementation because its state is all private
  context "#kcl_builder" do
    let(:builder) { kinesis.kcl_builder(queue) }

    it "sets the worker factory" do
      expect(field(builder, "recordProcessorFactory")).to_not eq(nil)
    end

    it "sets the config" do
      kinesis.register
      config = field(builder, "config")
      expect(config).to eq(kinesis.kcl_config)
    end

    it "disables metric tracking by default" do
      expect(field(builder, "metricsFactory")).to be_kind_of(com.amazonaws.services.kinesis.metrics.impl::NullMetricsFactory)
    end

    context "cloudwatch" do
      let(:metrics) { "cloudwatch" }
      it "uses cloudwatch metrics if specified" do
        # since the behaviour is enclosed on private methods it is not testable. So here
        # the expected value can be tested, not the result associated to set this value
        expect(field(builder, "metricsFactory")).to eq(nil)
      end
    end
  end

  def field(obj, name)
    field = obj.java_class.declared_field(name)
    field.accessible = true
    field.value(obj)
  end
end
