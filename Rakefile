require "bundler/gem_tasks"

begin
  require 'rspec/core/rake_task'
  RSpec::Core::RakeTask.new(:spec)
rescue LoadError
end

task default: "spec"

task "vendor_jars" do
  sh "mvn dependency:copy-dependencies -DoutputDirectory=vendor/jar-dependencies/runtime-jars/"
end

task build: "vendor_jars"
