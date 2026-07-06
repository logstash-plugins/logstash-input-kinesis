require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"

RSpec.describe "inputs/kinesis" do
  InitialPositionInStream = Java::software.amazon.kinesis.common::InitialPositionInStream

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
        "kinesis_endpoint" => "http://localhost:4567"
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

  # Config hash to test invalid additional_settings where the type is wrong
  let(:config_with_invalid_additional_settings_wrong_type) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "additional_settings" => {
        "initial_lease_table_read_capacity" => "not_a_number"
    }
  }}

  subject!(:kinesis) { LogStash::Inputs::Kinesis.new(config) }
  let(:kcl_scheduler) { double('kcl_scheduler') }
  let(:metrics) { nil }
  let(:codec) { LogStash::Codecs::JSON.new() }
  let(:queue) { Queue.new }

  it "registers without error" do
    input = LogStash::Plugin.lookup("input", "kinesis").new("kinesis_stream_name" => "specs", "codec" => codec)
    expect { input.register }.to_not raise_error
  end

  it "configures the KCL" do
    kinesis.register
    expect(kinesis.aws_credentials_provider.getClass.getName).to eq("software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
    expect(kinesis.lease_management_config.initialPositionInStream.getInitialPositionInStream).to eq(InitialPositionInStream::TRIM_HORIZON)
    # RetrievalConfig#initialPositionInStreamExtended stores the value in the streamTracker,
    # not in the like-named field its getter returns, so assert against the effective source.
    expect(kinesis.retrieval_config.streamTracker.streamConfigList.get(0).initialPositionInStreamExtended.getInitialPositionInStream).to eq(InitialPositionInStream::TRIM_HORIZON)
  end

  subject!(:kinesis_with_profile) { LogStash::Inputs::Kinesis.new(config_with_profile) }

  it "uses ProfileCredentialsProvider if profile is specified" do
    kinesis_with_profile.register
    expect(kinesis_with_profile.aws_credentials_provider.getClass.getName).to eq("software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider")
  end

  subject!(:kinesis_with_role_arn) { LogStash::Inputs::Kinesis.new(config_with_role_arn) }

  it "uses STS for accessing the kinesis stream if role_arn is specified" do
    kinesis_with_role_arn.register
    expect(kinesis_with_role_arn.aws_credentials_provider.getClass.getName).to eq("software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider")
  end

  subject!(:kinesis_with_latest) { LogStash::Inputs::Kinesis.new(config_with_latest) }

  it "configures the KCL with LATEST initial position" do
    kinesis_with_latest.register
    expect(kinesis_with_latest.lease_management_config.initialPositionInStream.getInitialPositionInStream).to eq(InitialPositionInStream::LATEST)
    expect(kinesis_with_latest.retrieval_config.streamTracker.streamConfigList.get(0).initialPositionInStreamExtended.getInitialPositionInStream).to eq(InitialPositionInStream::LATEST)
  end

  subject!(:kinesis_with_valid_additional_settings) { LogStash::Inputs::Kinesis.new(config_with_valid_additional_settings) }

  it "applies valid additional_settings to the matching config object" do
    kinesis_with_valid_additional_settings.register
    expect(kinesis_with_valid_additional_settings.lease_management_config.initialLeaseTableReadCapacity).to eq(25)
    expect(kinesis_with_valid_additional_settings.lease_management_config.initialLeaseTableWriteCapacity).to eq(100)
  end

  it "consumes kinesis_endpoint from additional_settings as a client endpoint override" do
    # KCL 1.x accepted `kinesis_endpoint` via additional_settings; in the SDK v2 it
    # becomes a client endpoint override, so it must be consumed rather than raise.
    kinesis_with_valid_additional_settings.register
    expect(kinesis_with_valid_additional_settings.kinesis_client).to_not be_nil
    expect(kinesis_with_valid_additional_settings.additional_settings).to_not include("kinesis_endpoint")
  end

  # Config to test settings that KCL 2.x moved onto PollingConfig
  let(:config_with_polling_settings) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "additional_settings" => {
        "max_records" => 5000,
        "retry_get_records_in_seconds" => 5
    }
  }}
  subject!(:kinesis_with_polling_settings) { LogStash::Inputs::Kinesis.new(config_with_polling_settings) }

  it "routes PollingConfig settings from additional_settings" do
    kinesis_with_polling_settings.register
    # max_records is a plain int setter; retry_get_records_in_seconds takes an
    # Optional<Integer> in KCL 2.x but a bare int in 1.x, so the compatibility
    # shim must wrap it.
    expect(kinesis_with_polling_settings.polling_config.maxRecords).to eq(5000)
    expect(kinesis_with_polling_settings.polling_config.retryGetRecordsInSeconds.get).to eq(5)
  end

  # A "kitchen sink" config exercising every additional_settings target at once:
  # client endpoint overrides, PollingConfig (including the Optional-typed setters),
  # and at least one setting per KCL 2.x config object.
  let(:config_with_all_additional_settings) {{
    "application_name" => "my-processor",
    "kinesis_stream_name" => "run-specs",
    "codec" => codec,
    "metrics" => metrics,
    "checkpoint_interval_seconds" => 120,
    "region" => "ap-southeast-1",
    "profile" => nil,
    "additional_settings" => {
        # client endpoint overrides (KCL 1.x compatibility)
        "kinesis_endpoint" => "http://localhost:4567",
        "dynamodb_endpoint" => "http://localhost:8000",
        # PollingConfig (bare int + Optional<Integer> setters)
        "max_records" => 5000,
        "retry_get_records_in_seconds" => 5,
        "max_get_records_thread_pool" => 4,
        # LeaseManagementConfig
        "initial_lease_table_read_capacity" => 25,
        "initial_lease_table_write_capacity" => 100,
        "failover_time_millis" => 15000,
        # CoordinatorConfig
        "parent_shard_poll_interval_millis" => 20000,
        # ProcessorConfig
        "call_process_records_even_for_empty_record_list" => true,
        # MetricsConfig
        "metrics_buffer_time_millis" => 5000,
        "metrics_max_queue_size" => 1000,
        # LifecycleConfig
        "task_backoff_time_millis" => 1000
    }
  }}
  subject!(:kinesis_with_all_additional_settings) { LogStash::Inputs::Kinesis.new(config_with_all_additional_settings) }

  it "applies every supported additional_settings key without error" do
    expect { kinesis_with_all_additional_settings.register }.to_not raise_error

    # endpoints consumed as client overrides (removed from additional_settings)
    expect(kinesis_with_all_additional_settings.kinesis_client).to_not be_nil
    expect(kinesis_with_all_additional_settings.dynamo_db_client).to_not be_nil
    expect(kinesis_with_all_additional_settings.additional_settings).to_not include("kinesis_endpoint")
    expect(kinesis_with_all_additional_settings.additional_settings).to_not include("dynamodb_endpoint")

    # PollingConfig
    expect(kinesis_with_all_additional_settings.polling_config.maxRecords).to eq(5000)
    expect(kinesis_with_all_additional_settings.polling_config.retryGetRecordsInSeconds.get).to eq(5)
    expect(kinesis_with_all_additional_settings.polling_config.maxGetRecordsThreadPool.get).to eq(4)

    # LeaseManagementConfig
    expect(kinesis_with_all_additional_settings.lease_management_config.initialLeaseTableReadCapacity).to eq(25)
    expect(kinesis_with_all_additional_settings.lease_management_config.initialLeaseTableWriteCapacity).to eq(100)
    expect(kinesis_with_all_additional_settings.lease_management_config.failoverTimeMillis).to eq(15000)

    # CoordinatorConfig / ProcessorConfig / MetricsConfig / LifecycleConfig
    expect(kinesis_with_all_additional_settings.coordinator_config.parentShardPollIntervalMillis).to eq(20000)
    expect(kinesis_with_all_additional_settings.processor_config.callProcessRecordsEvenForEmptyRecordList).to eq(true)
    expect(kinesis_with_all_additional_settings.metrics_config.metricsBufferTimeMillis).to eq(5000)
    expect(kinesis_with_all_additional_settings.metrics_config.metricsMaxQueueSize).to eq(1000)
    expect(kinesis_with_all_additional_settings.lifecycle_config.taskBackoffTimeMillis).to eq(1000)
  end

  subject!(:kinesis_with_proxy) { LogStash::Inputs::Kinesis.new(config_with_proxy) }

  it "configures the clients with proxy settings" do
    kinesis_with_proxy.register
    proxy_uri = kinesis_with_proxy.send(:extract_proxy_uri)

    # Netty (async) proxy configuration for Kinesis/DynamoDB/CloudWatch.
    proxy_config = kinesis_with_proxy.send(:build_proxy_configuration, proxy_uri)
    expect(proxy_config.username).to eq("user1")
    expect(proxy_config.host).to eq("proxy.example.com")
    expect(proxy_config.port).to eq(3128)
    expect(proxy_config.nonProxyHosts.to_a).to eq(["127.0.0.5"])

    # Apache (sync) proxy configuration for the STS client used by role assumption.
    apache_proxy = kinesis_with_proxy.send(:build_apache_proxy_configuration, proxy_uri)
    expect(apache_proxy.username).to eq("user1")
    expect(apache_proxy.host).to eq("proxy.example.com")
    expect(apache_proxy.port).to eq(3128)
    expect(apache_proxy.nonProxyHosts.to_a).to eq(["127.0.0.5"])
  end

  subject!(:kinesis_with_invalid_additional_settings_name_not_found) { LogStash::Inputs::Kinesis.new(config_with_invalid_additional_settings_name_not_found) }

  it "raises NoMethodError for invalid configuration options" do
    expect{ kinesis_with_invalid_additional_settings_name_not_found.register }.to raise_error(NoMethodError)
  end

  subject!(:kinesis_with_invalid_additional_settings_wrong_type) { LogStash::Inputs::Kinesis.new(config_with_invalid_additional_settings_wrong_type) }

  it "raises a descriptive error for invalid configuration values such as the wrong type" do
    expect{ kinesis_with_invalid_additional_settings_wrong_type.register }
      .to raise_error(/Invalid additional_settings value for 'initial_lease_table_read_capacity'/)
  end

  context "#run" do
    it "runs the KCL scheduler" do
      expect(kinesis).to receive(:build_scheduler).and_return(kcl_scheduler)
      expect(kcl_scheduler).to receive(:run).with(no_args)
      kinesis.run(queue)
    end
  end

  context "#stop" do
    it "stops the KCL scheduler" do
      expect(kinesis).to receive(:build_scheduler).and_return(kcl_scheduler)
      expect(kcl_scheduler).to receive(:run).with(no_args)
      expect(kcl_scheduler).to receive(:shutdown).with(no_args)
      kinesis.run(queue)
      kinesis.do_stop # do_stop calls stop internally
    end
  end

  context "#worker_factory" do
    before { kinesis.instance_variable_set(:@output_queue, queue) }

    it "clones the codec for each worker" do
      worker = kinesis.worker_factory.call()
      expect(worker).to be_kind_of(LogStash::Inputs::Kinesis::Worker)
      expect(worker.codec).to_not eq(kinesis.codec)
      expect(worker.codec).to be_kind_of(codec.class)
    end

    it "generates a valid worker" do
      worker = kinesis.worker_factory.call()

      expect(worker.codec).to be_kind_of(codec.class)
      expect(worker.checkpoint_interval).to eq(120)
      expect(worker.output_queue).to eq(queue)
      expect(worker.decorator).to eq(kinesis.method(:decorate))
      expect(worker.logger).to eq(kinesis.logger)
    end
  end
end
