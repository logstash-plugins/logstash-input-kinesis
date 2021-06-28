require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"

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

  # Config hash to test assume role provider if role_arn is specified
  let(:config_with_role_arn) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "role_arn" => "arn:aws:iam::???????????:role/my-role"
  }}

  # other config with LATEST as initial_position_in_stream
  let(:config_with_latest) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "initial_position_in_stream" => "LATEST"
  }}

  # Config hash to test valid additional_settings
  let(:config_with_valid_additional_settings) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "additional_settings" => {
        "initial_lease_table_read_capacity" => 25,
        "initial_lease_table_write_capacity" => 100,
        "kinesis_endpoint" => "http://localhost"
    }
  }}

  # Config with proxy
  let(:config_with_proxy) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "http_proxy" => "http://user1:pwd1@proxy.example.com:3128/",
    "no_proxy" => "127.0.0.5",
  }}

  # Config hash to test invalid additional_settings where the name is not found
  let(:config_with_invalid_additional_settings_name_not_found) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "additional_settings" => {
        "foo" => "bar"
    }
  }}

  # Config hash to test invalid additional_settings where the type is complex or wrong
  let(:config_with_invalid_additional_settings_wrong_type) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "additional_settings" => {
        "metrics_level" => "invalid_metrics_level"
    }
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

  subject!(:kinesis_with_role_arn) { LogStash::Inputs::Kinesis.new(config_with_role_arn) }

  it "uses STS for accessing the kinesis stream if role_arn is specified" do
    kinesis_with_role_arn.register
    expect(kinesis_with_role_arn.kcl_config.get_kinesis_credentials_provider.getClass.to_s).to eq("com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider")
    expect(kinesis_with_role_arn.kcl_config.get_dynamo_db_credentials_provider.getClass.to_s).to eq("com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider")
    expect(kinesis_with_role_arn.kcl_config.get_cloud_watch_credentials_provider.getClass.to_s).to eq("com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider")
  end

  subject!(:kinesis_with_latest) { LogStash::Inputs::Kinesis.new(config_with_latest) }

  it "configures the KCL" do
    kinesis_with_latest.register
    expect(kinesis_with_latest.kcl_config.applicationName).to eq("my-processor")
    expect(kinesis_with_latest.kcl_config.streamName).to eq("run-specs")
    expect(kinesis_with_latest.kcl_config.regionName).to eq("ap-southeast-1")
    expect(kinesis_with_latest.kcl_config.initialPositionInStream).to eq(KCL::InitialPositionInStream::LATEST)
    expect(kinesis_with_latest.kcl_config.get_kinesis_credentials_provider.getClass.to_s).to eq("com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
  end

  subject!(:kinesis_with_valid_additional_settings) { LogStash::Inputs::Kinesis.new(config_with_valid_additional_settings) }

  it "configures the KCL" do
    kinesis_with_valid_additional_settings.register
    expect(kinesis_with_valid_additional_settings.kcl_config.applicationName).to eq("my-processor")
    expect(kinesis_with_valid_additional_settings.kcl_config.streamName).to eq("run-specs")
    expect(kinesis_with_valid_additional_settings.kcl_config.regionName).to eq("ap-southeast-1")
    expect(kinesis_with_valid_additional_settings.kcl_config.initialLeaseTableReadCapacity).to eq(25)
    expect(kinesis_with_valid_additional_settings.kcl_config.initialLeaseTableWriteCapacity).to eq(100)
    expect(kinesis_with_valid_additional_settings.kcl_config.kinesisEndpoint).to eq("http://localhost")
  end

  subject!(:kinesis_with_proxy) { LogStash::Inputs::Kinesis.new(config_with_proxy) }

  it "configures the KCL with proxy settings" do
    kinesis_with_proxy.register
    clnt_config = kinesis_with_proxy.kcl_config.kinesis_client_configuration
    expect(clnt_config.get_proxy_username).to eq("user1")
    expect(clnt_config.get_proxy_host).to eq("proxy.example.com")
    expect(clnt_config.get_proxy_port).to eq(3128)
    expect(clnt_config.get_non_proxy_hosts).to eq("127.0.0.5")
  end

  subject!(:kinesis_with_invalid_additional_settings_name_not_found) { LogStash::Inputs::Kinesis.new(config_with_invalid_additional_settings_name_not_found) }

  it "raises NoMethodError for invalid configuration options" do
    expect{ kinesis_with_invalid_additional_settings_name_not_found.register }.to raise_error(NoMethodError)
  end


  subject!(:kinesis_with_invalid_additional_settings_wrong_type) { LogStash::Inputs::Kinesis.new(config_with_invalid_additional_settings_wrong_type) }

  it "raises an error for invalid configuration values such as the wrong type" do
    expect{ kinesis_with_invalid_additional_settings_wrong_type.register }.to raise_error(Java::JavaLang::IllegalArgumentException)
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
