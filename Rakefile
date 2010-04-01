require 'open-uri'
require 'uri'

task :default => [:test]

def cassandra_running?
  File.exists?(Dir.pwd + "/cassandra/cassandra.pid")
end


namespace :cassandra do
  version = "0.6.0"
  subversion = "-rc1"
  url = "http://www.nic.funet.fi/pub/mirrors/apache.org/cassandra/#{version}/apache-cassandra-#{version}#{subversion}-bin.tar.gz"

  desc "Setup Cassandra"
  task :setup do
    raise "Cassandra is already installed" if File.exists?(Dir.pwd + "/cassandra")
puts url
    sh "tar -zxf #{URI.parse(url).open.path}"
    sh "mv apache-cassandra-#{version}#{subversion} cassandra"
    sh "mv cassandra/conf cassandra/default_conf"
    sh "ln -nfs #{Dir.pwd}/config cassandra/conf"
  end
  
  desc "Cleanup cassandra files"
  task :cleanup do
    FileUtils.rm_r(Dir.pwd + '/data') if File.exists?(Dir.pwd + "/data")
    FileUtils.rm_r(Dir.pwd + '/log') if File.exists?(Dir.pwd + "/log")
  end
  
  desc "Start Cassandra"
  task :start do
    raise "Cassandra already running" if cassandra_running?
    Rake::Task["cassandra:setup"].execute unless File.exists?(Dir.pwd + "/cassandra")
    ENV["CASSANDRA_INCLUDE"] = "./config/cassandra.in.sh"
    sh "./cassandra/bin/cassandra -p #{Dir.pwd + "/cassandra/cassandra.pid"}"
  end
  
  desc "Stop Cassandra"
  task :stop do
    raise "Cassandra not running" unless cassandra_running?
    sh "kill #{File.open("cassandra/cassandra.pid").read}"
  end
end

desc "Invoke SBT"
task :sbt, :command do |task, command|
  c = command.class <= String ? command : command["command"]
  sh "java -Xmx512M -jar bin/sbt-launch-0.7.1.jar #{c}"
end

desc "Compile Scalandra"
task :compile do
  Rake::Task["sbt"].execute("compile")
end

desc "Fetch dependencies"
task :dependencies do
  Rake::Task["sbt"].execute("update")
end

desc "Test scalandra"
task :test => :dependencies do
  begin
    Rake::Task["cassandra:stop"].execute if cassandra_running?
    Rake::Task["cassandra:cleanup"].execute
    Rake::Task["cassandra:start"].execute
    Rake::Task["sbt"].execute("test")
  ensure
    Rake::Task["cassandra:stop"].execute
  end
end