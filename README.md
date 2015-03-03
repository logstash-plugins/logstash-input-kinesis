# Logstash AWS Kinesis Input Plugin

This is a plugin for [Logstash](https://github.com/elasticsearch/logstash).

## Installation

This plugin requires Logstash 1.5, and can be installed by Logstash
itself.

```sh
$(LOGSTASH_DIR)/bin/plugin install logstash-input-kinesis
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

At least `kinesis_stream_name` is requred.

## Contributing

1. Fork it ( https://github.com/codekitchen/logstash-input-kinesis/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
