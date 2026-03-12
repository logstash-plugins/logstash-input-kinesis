# Logstash AWS Kinesis Input Plugin

[![Build Status](https://travis-ci.com/logstash-plugins/logstash-input-kinesis.svg)](https://travis-ci.com/logstash-plugins/logstash-input-kinesis)

This is a [AWS Kinesis](http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html) input plugin for [Logstash](https://github.com/elasticsearch/logstash). Under the hood uses the [Kinesis Client Library](http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-implementation-app-java.html).

## Installation

This plugin requires Logstash >= 2.0, and can be installed by Logstash
itself.

```sh
bin/logstash-plugin install logstash-input-kinesis
```

## Usage

```
input {
  kinesis {
    kinesis_stream_name => "my-logging-stream"
    codec => json { }
  }
}
```

### Using with CloudWatch Logs

If you are looking to read a CloudWatch Logs subscription stream, you'll also want to install and configure the [CloudWatch Logs Codec](https://github.com/threadwaste/logstash-codec-cloudwatch_logs).

## Configuration

This are the properties you can configure and what are the default values:

* `application_name`: The name of the application used in DynamoDB for coordination. Only one worker per unique stream partition and application will be actively consuming messages.
    * **required**: false
    * **default value**: `logstash`
* `kinesis_stream_name`: The Kinesis stream name.
    * **required**: true
* `region`: The AWS region name for Kinesis, DynamoDB and Cloudwatch (if enabled)
    * **required**: false
    * **default value**: `us-east-1`
* `checkpoint_interval_seconds`: How many seconds between worker checkpoints to DynamoDB. A low value ussually means lower message replay in case of node failure/restart but it increases CPU+network ussage (which increases the AWS costs).
    * **required**: false
    * **default value**: `60`
* `metrics`: Worker metric tracking. By default this is disabled, set it to "cloudwatch" to enable the cloudwatch integration in the Kinesis Client Library.
    * **required**: false
    * **default value**: `nil`
* `profile`: The AWS profile name for authentication. This ensures that the `~/.aws/credentials` AWS auth provider is used. By default this is empty and the default chain will be used.
    * **required**: false
* `role_arn`: The AWS role to assume. This can be used, for example, to access a Kinesis stream in a different AWS
account. This role will be assumed after the default credentials or profile credentials are created. By default
this is empty and a role will not be assumed.
    * **required**: false
* `role_session_name`: Session name to use when assuming an IAM role. This is recorded in CloudTrail logs for example.
    * **required**: false
    * **default value**: `"logstash"`
* `initial_position_in_stream`: The value for initialPositionInStream. Accepts "TRIM_HORIZON" or "LATEST".
    * **required**: false
    * **default value**: `"TRIM_HORIZON"`

### Additional KCL Settings
* `additional_settings`: KCL v2 splits configuration across several sub-config objects: `CheckpointConfig`, `CoordinatorConfig`, `LeaseManagementConfig`, `LifecycleConfig`, `MetricsConfig`, `ProcessorConfig`, and `RetrievalConfig`. You can set any primitive property on these objects by providing key-value pairs using the Java method name (camelCase) or JRuby-style snake_case. The plugin automatically finds the correct sub-config object for each setting. For example, to set the DynamoDB lease table read and write capacity values (properties on `LeaseManagementConfig`), provide a hash of `additional_settings => {"initialLeaseTableReadCapacity" => 25, "initialLeaseTableWriteCapacity" => 100}`
    * **required**: false
    * **default value**: `{}`

## Authentication

This plugin uses the default AWS SDK auth chain, [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html), to determine which credentials the client will use, unless `profile` is set, in which case [ProfileCredentialsProvider](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/profile/ProfileCredentialsProvider.html) is used.

The default chain follows this order trying to read the credentials:
 * `AWS_ACCESS_KEY_ID` / `AWS_SECRET_KEY` environment variables
 * `~/.aws/credentials` credentials file
 * EC2 instance profile

The credentials will need access to the following services:
* AWS Kinesis
* AWS DynamoDB: the client library stores information for worker coordination in DynamoDB (offsets and active worker per partition)
* AWS CloudWatch: if the metrics are enabled the credentials need CloudWatch update permisions granted.

Look at the [documentation](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) for deeper information on the default chain.

## Contributing

0. https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md#contribution-steps
1. Fork it ( https://github.com/logstash-plugins/logstash-input-kinesis/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request


## Development

To download all jars: 
`bundler exec rake install_jars`

To run all specs: 
`bundler exec rspec`

### Integration Tests

Integration tests are available that validate the plugin build, installation, and initialization process:

```sh
make integration
```

This will:
- Build the gem package
- Install the plugin in a Logstash container
- Verify the plugin registers and initializes correctly
- Test with LocalStack infrastructure

**Note**: The plugin now uses Kinesis Client Library v2.7.2, which supports custom endpoint configuration for LocalStack integration. The test validates critical integration points (build, install, register, initialize) which catches most common issues.

For full end-to-end testing with LocalStack or real AWS credentials with actual Kinesis streams, see [integration-test/README.md](integration-test/README.md).