require "bundler/gem_tasks"

begin
  require 'rspec/core/rake_task'
  RSpec::Core::RakeTask.new(:spec)
rescue LoadError
end

task default: "spec"

require 'jars/installer'
desc 'Install the JAR dependencies to vendor/'
task :install_jars do
  # If we don't have these env variables set, jar-dependencies will
  # download the jars and place it in $PWD/lib/. We actually want them in
  # $PWD/vendor
  ENV['JARS_HOME'] = Dir.pwd + "/vendor/jar-dependencies/runtime-jars"
  ENV['JARS_VENDOR'] = "false"
  Jars::Installer.new.vendor_jars!(false)
end

task build: :install_jars
