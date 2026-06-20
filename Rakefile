require "bundler/gem_tasks"
require 'jars/version'

begin
  require 'rspec/core/rake_task'
  RSpec::Core::RakeTask.new(:spec)
rescue LoadError
end

task default: "spec"

require 'jars/installer'

# Logstash pins jar-dependencies to 0.4.1, which predates the fix for newer Maven
# (ruby-maven-libs) appending `-- module <name> (auto)` and ANSI color codes after
# each jar path in its `dependency:list` output. That trailing text corrupts the
# parsed jar filename and makes `rake vendor` fail with Errno::ENOENT. Strip it
# before parsing. Mirrors the workaround in elastic/logstash#16919.
class ::Jars::Installer
  def self.load_from_maven(file)
    result = []
    ::File.read(file).each_line do |line|
      sanitized = line.gsub(/\e\[[0-9;]*m/, '').sub(/\s--\s.*$/, '').strip
      dep = ::Jars::Installer::Dependency.new(sanitized)
      result << dep if dep && dep.scope == :runtime
    end
    result
  end
end

desc 'Install the JAR dependencies to vendor/'
task :install_jars do
  # We actually want jar-dependencies will download the jars and place it in
  # vendor/jar-dependencies/runtime-jars
  Jars::Installer.new.vendor_jars!(false, 'vendor/jar-dependencies/runtime-jars')
end

task build: :install_jars
require "logstash/devutils/rake"
task vendor: :install_jars

