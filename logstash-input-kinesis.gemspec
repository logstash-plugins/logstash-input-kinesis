# coding: utf-8
version = File.read(File.expand_path(File.join(File.dirname(__FILE__), "VERSION"))).strip

Gem::Specification.new do |spec|
  spec.name          = "logstash-input-kinesis"
  spec.version       = version
  spec.authors       = ["Brian Palmer"]
  spec.email         = ["brian@codekitchen.net"]
  spec.summary       = "Receives events through an AWS Kinesis stream"
  spec.description   = %q{This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program}
  spec.homepage      = "https://github.com/logstash-plugins/logstash-input-kinesis"
  spec.licenses      = ['Apache-2.0']

  spec.files         = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib', 'vendor/jar-dependencies/runtime-jars']

  # Special flag to let us know this is actually a logstash plugin
  spec.metadata      = { "logstash_plugin" => "true", "logstash_group" => "input" }

  spec.platform      = 'java'

  spec.requirements << "jar 'com.amazonaws:amazon-kinesis-client', '1.9.2'"
  spec.requirements << "jar 'com.amazonaws:aws-java-sdk-core', '1.11.414'"
  spec.requirements << "jar 'com.amazonaws:aws-java-sdk-sts', '1.11.414'"

  spec.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"

  spec.add_development_dependency 'logstash-devutils'
  spec.add_development_dependency 'jar-dependencies', '~> 0.4'
  spec.add_development_dependency "logstash-codec-json"
end
