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
  # We want jar-dependencies to download the jars and place them in
  # vendor/jar-dependencies/runtime-jars.
  vendor_dir = 'vendor/jar-dependencies/runtime-jars'
  installer = Jars::Installer.new

  # The `vendor_jars!` signature differs across jar-dependencies versions: 0.4.x (Logstash-pinned) takes
  # `vendor_jars!(write_require_file = true, vendor_dir = nil)` while 0.5.x takes
  # `vendor_jars!(vendor_dir = nil, write_require_file: true)`. Detect which one
  # is present so `rake vendor` works under both (e.g. newer local JRubies).
  if installer.method(:vendor_jars!).parameters.include?([:key, :write_require_file])
    installer.vendor_jars!(vendor_dir, write_require_file: false)
  else
    installer.vendor_jars!(false, vendor_dir)
  end
end

task build: :install_jars
require "logstash/devutils/rake"
task vendor: :install_jars

