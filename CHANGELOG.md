## 2.1.2
  - Fixed logging level setup to use JUL and JCL loggers backend [#87](https://github.com/logstash-plugins/logstash-input-kinesis/issue/87)

## 2.1.1
  - Added helpful content from readme to doc file for publishing [#63](https://github.com/logstash-plugins/logstash-input-kinesis/pull/63)

## 2.1.0
  - Changed role assumption to also assume role for interactions with dynamodb and cloudwatch [#66](https://github.com/logstash-plugins/logstash-input-kinesis/pull/66)

## 2.0.11
  - Added the ability to assume a role [#40](https://github.com/logstash-plugins/logstash-input-kinesis/pull/40)

## 2.0.10
  - Added the ability to set additional settings exposed through KinesisClientLibConfiguration [#51](https://github.com/logstash-plugins/logstash-input-kinesis/pull/51)

## 2.0.9
  - Changed the 'workerid' to also include the host of the Logstash node [#48](https://github.com/logstash-plugins/logstash-input-kinesis/pull/48)

## 2.0.8
  - Changed plugin to use more recent versions of Kinesis Client library and AWS SDK[#45](https://github.com/logstash-plugins/logstash-input-kinesis/pull/45)

## 2.0.7
  - Docs: Set the default_codec doc attribute.

## 2.0.7
  - Update gemspec summary

## 2.0.6
 - Fix some documentation issues
 - Add support for `initial_position_in_stream` config parameter. `TRIM_HORIZON` and `LATEST` are supported.

## 2.0.5
 - Docs: Add CHANGELOG.md
 - Support for specifying an AWS credentials profile with the `profile` config parameter
 - Docs: Remove extraneous text added during doc extract

## 2.0.4
 -  Docs: Bump version for automated doc build

## 2.0.3
 -  Fix error about failed to coerce java.util.logging.Level to org.apache.log4j.Level with logstash 5.1.1

## 2.0.2
 -  Fix error with Logstash 5.0
 
## 2.0.1
 -  Add partition_key, approximate_arrival_timestamp and sequence_number fields in the @metadata sub-has
