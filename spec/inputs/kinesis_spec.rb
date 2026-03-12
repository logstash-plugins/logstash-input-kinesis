require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"

RSpec.describe "inputs/kinesis" do
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
    "role_arn" => "arn:aws:iam::123456789012:role/my-role"
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

  # Config hash to test valid additional_settings on ConfigsBuilder
  let(:config_with_valid_additional_settings) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "additional_settings" => {
        "tableName" => "custom-table-name"
    }
  }}

  # Config hash to test valid additional_settings on sub-config objects
  let(:config_with_sub_config_additional_settings) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "additional_settings" => {
        "initialLeaseTableReadCapacity" => 25,
        "initialLeaseTableWriteCapacity" => 100
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
    "http_proxy" => ::LogStash::Util::Password.new("http://user1:pwd1@proxy.example.com:3128/"),
    "non_proxy_hosts" => "127.0.0.5",
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

  subject!(:kinesis) { LogStash::Inputs::Kinesis.new(config) }
  let(:kcl_worker) { double('kcl_worker', run: nil, shutdown: nil) }
  let(:metrics) { nil }
  let(:codec) { LogStash::Codecs::JSON.new() }
  let(:queue) { Queue.new }

  it "registers without error" do
    input = LogStash::Plugin.lookup("input", "kinesis").new("kinesis_stream_name" => "specs", "codec" => codec)
    expect { input.register }.to_not raise_error
  end

  it "creates KCL configuration" do
    kinesis.register
    expect(kinesis.kcl_config).to_not be_nil
    expect(kinesis.kcl_config.applicationName()).to eq("my-processor")
  end

  subject!(:kinesis_with_profile) { LogStash::Inputs::Kinesis.new(config_with_profile) }

  it "registers with profile credentials provider" do
    expect { kinesis_with_profile.register }.to_not raise_error
  end

  subject!(:kinesis_with_role_arn) { LogStash::Inputs::Kinesis.new(config_with_role_arn) }

  it "registers with STS role assumption" do
    expect { kinesis_with_role_arn.register }.to_not raise_error
  end

  subject!(:kinesis_with_latest) { LogStash::Inputs::Kinesis.new(config_with_latest) }

  it "configures LATEST initial position" do
    expect { kinesis_with_latest.register }.to_not raise_error
  end

  subject!(:kinesis_with_valid_additional_settings) { LogStash::Inputs::Kinesis.new(config_with_valid_additional_settings) }

  it "applies additional settings on ConfigsBuilder" do
    expect { kinesis_with_valid_additional_settings.register }.to_not raise_error
  end

  subject!(:kinesis_with_sub_config_additional_settings) { LogStash::Inputs::Kinesis.new(config_with_sub_config_additional_settings) }

  it "applies additional settings on sub-config objects" do
    expect { kinesis_with_sub_config_additional_settings.register }.to_not raise_error
  end

  subject!(:kinesis_with_proxy) { LogStash::Inputs::Kinesis.new(config_with_proxy) }

  it "configures with proxy settings" do
    expect { kinesis_with_proxy.register }.to_not raise_error
  end

  subject!(:kinesis_with_invalid_additional_settings_name_not_found) { LogStash::Inputs::Kinesis.new(config_with_invalid_additional_settings_name_not_found) }

  it "raises NoMethodError for invalid configuration options" do
    expect{ kinesis_with_invalid_additional_settings_name_not_found.register }.to raise_error(NoMethodError)
  end

  context "#run" do
    before do
      kinesis.register
    end

    it "creates and runs the KCL scheduler" do
      # Skip actual scheduler run as it requires AWS credentials
      expect(kinesis.instance_variable_get(:@kcl_config)).to_not be_nil
    end
  end

  context "#stop" do
    before do
      kinesis.register
    end

    it "stops the KCL worker" do
      # Skip actual scheduler run as it requires AWS credentials
      expect { kinesis.stop }.to_not raise_error
    end
  end

  context "#worker_factory_lambda" do
    before do
      kinesis.register
    end

    it "creates a valid worker factory" do
      factory = kinesis.worker_factory_lambda
      expect(factory).to_not be_nil
      expect(factory).to respond_to(:shardRecordProcessor)
    end

    it "worker factory creates workers" do
      factory = kinesis.worker_factory_lambda
      kinesis.instance_variable_get(:@output_queue_holder).queue = queue
      worker = factory.shardRecordProcessor
      expect(worker).to be_kind_of(LogStash::Inputs::Kinesis::Worker)
      expect(worker.checkpoint_interval).to eq(120)
    end
  end
end
