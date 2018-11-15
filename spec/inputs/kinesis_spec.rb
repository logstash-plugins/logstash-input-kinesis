require "logstash/plugin"
require "logstash/inputs/kinesis"
require "logstash/codecs/json"

RSpec.describe "inputs/kinesis" do
  KCL = software.amazon.kinesis.common

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

  subject!(:kinesis) { LogStash::Inputs::Kinesis.new(config) }
  let(:kcl_worker) { double('kcl_worker') }
  let(:metrics) { nil }
  let(:codec) { LogStash::Codecs::JSON.new() }
  let(:queue) { Queue.new }

  subject!(:kinesis) { LogStash::Inputs::Kinesis.new(config) }

  it "registers without error" do
    input = LogStash::Plugin.lookup("input", "kinesis").new("kinesis_stream_name" => "specs", "codec" => codec)
    expect { input.register }.to_not raise_error
  end

  it "configures the KCL" do
    kinesis.register
    expect(kinesis.kcl_config.applicationName).to eq("my-processor")
    expect(kinesis.kcl_config.streamName).to eq("run-specs")
    expect(kinesis.retrieval_config.initialPositionInStreamExtended.initialPositionInStream).to eq(KCL::InitialPositionInStream::TRIM_HORIZON)

    assert_client_region = -> (client) do
      config = get_client_configuration client
      expect(config.option(software.amazon.awssdk.awscore.client.config.AwsClientOption::AWS_REGION).to_s).to eq("ap-southeast-1")
    end

    assert_client_region.call kinesis.kcl_config.cloudWatchClient
    assert_client_region.call kinesis.kcl_config.dynamoDBClient
    assert_client_region.call kinesis.kcl_config.kinesisClient

    assert_credentials_provider = -> (client) do
      config = get_client_configuration client
      expect(config.option(software.amazon.awssdk.awscore.client.config.AwsClientOption::CREDENTIALS_PROVIDER).getClass.to_s).to eq("software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
    end

    assert_credentials_provider.call kinesis.kcl_config.cloudWatchClient
    assert_credentials_provider.call kinesis.kcl_config.dynamoDBClient
    assert_credentials_provider.call kinesis.kcl_config.kinesisClient
  end

  subject!(:kinesis_with_profile) { LogStash::Inputs::Kinesis.new(config_with_profile) }

  it "uses ProfileCredentialsProvider if profile is specified" do
    kinesis_with_profile.register

    assert_credentials_provider = -> (client) do
      config = get_client_configuration client
      expect(config.option(software.amazon.awssdk.awscore.client.config.AwsClientOption::CREDENTIALS_PROVIDER).getClass.to_s).to eq("software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider")
	end

    assert_credentials_provider.call kinesis_with_profile.kcl_config.cloudWatchClient
    assert_credentials_provider.call kinesis_with_profile.kcl_config.dynamoDBClient
    assert_credentials_provider.call kinesis_with_profile.kcl_config.kinesisClient
  end

  subject!(:kinesis_with_latest) { LogStash::Inputs::Kinesis.new(config_with_latest) }

  it "configures the KCL" do
     kinesis_with_latest.register

     expect(kinesis_with_latest.kcl_config.applicationName).to eq("my-processor")
     expect(kinesis_with_latest.kcl_config.streamName).to eq("run-specs")
     expect(kinesis_with_latest.retrieval_config.initialPositionInStreamExtended.initialPositionInStream).to eq(KCL::InitialPositionInStream::LATEST)

     assert_client_region = -> (client) do
       config = get_client_configuration client
       expect(config.option(software.amazon.awssdk.awscore.client.config.AwsClientOption::AWS_REGION).to_s).to eq("ap-southeast-1")
     end

     assert_client_region.call kinesis_with_latest.kcl_config.cloudWatchClient
     assert_client_region.call kinesis_with_latest.kcl_config.dynamoDBClient
     assert_client_region.call kinesis_with_latest.kcl_config.kinesisClient

     assert_credentials_provider = -> (client) do
       config = get_client_configuration client
       expect(config.option(software.amazon.awssdk.awscore.client.config.AwsClientOption::CREDENTIALS_PROVIDER).getClass.to_s).to eq("software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
     end

     assert_credentials_provider.call kinesis_with_latest.kcl_config.cloudWatchClient
     assert_credentials_provider.call kinesis_with_latest.kcl_config.dynamoDBClient
     assert_credentials_provider.call kinesis_with_latest.kcl_config.kinesisClient
  end

  it "starts the KCL worker" do
    expect(kinesis).to receive(:kcl_builder).with(queue).and_return(kcl_worker)
    expect(kcl_worker).to receive(:run).with(no_args)
    kinesis.run(queue)
  end

  it "stops the KCL worker" do
    expect(kinesis).to receive(:kcl_builder).with(queue).and_return(kcl_worker)
    expect(kcl_worker).to receive(:run).with(no_args)
    expect(kcl_worker).to receive(:start_graceful_shutdown).with(no_args).and_return(java.util.concurrent.CompletableFuture.completed_future(true))
    kinesis.run(queue)
    kinesis.do_stop # do_stop calls stop internally
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
    let(:scheduler) {
      kinesis.register
      scheduler = kinesis.kcl_builder(queue)

      # https://github.com/awslabs/amazon-kinesis-client/issues/464
      scheduler.metrics_factory.shutdown if scheduler.metrics_factory.is_a?(software.amazon.kinesis.metrics::CloudWatchMetricsFactory)

      scheduler
    }

    it "sets the worker factory" do
      expect(field(scheduler, "processorConfig").shardRecordProcessorFactory).to_not eq(nil)
    end

    it "disables metric tracking by default" do
      expect(field(scheduler, "metricsFactory")).to be_kind_of(software.amazon.kinesis.metrics::NullMetricsFactory)
    end

    context "cloudwatch" do
      let(:metrics) { "cloudwatch" }
      it "uses cloudwatch metrics if specified" do
        # since the behaviour is enclosed on private methods it is not testable. So here
        # the expected value can be tested, not the result associated to set this value
        expect(field(scheduler, "metricsFactory")).to be_kind_of(software.amazon.kinesis.metrics::CloudWatchMetricsFactory)
      end
    end
  end

  def software
    Java::Software
  end

  def get_client_configuration(client)
    handler = field(client, 'clientHandler')
    field(handler, 'clientConfiguration')
  end

  def field(obj, name)
    field = obj.java_class.declared_field(name)
    field.accessible = true
    field.value(obj)
  end
end
