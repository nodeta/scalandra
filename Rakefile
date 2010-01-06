require 'open-uri'
require 'uri'

task :default => [:test]

def cassandra_running?
  File.exists?(Dir.pwd + "/cassandra/cassandra.pid")
end


namespace :cassandra do
  version = "0.5.0-rc3"
  url = "http://people.apache.org/~eevans/apache-cassandra-incubating-#{version}-bin.tar.gz"

  desc "Setup Cassandra"
  task :setup do
    raise "Cassandra is already installed" if File.exists?(Dir.pwd + "/cassandra")
    sh "tar -zxf #{URI.parse(url).open.path}"
    sh "mv apache-cassandra-incubating-#{version} cassandra"
    sh "mv cassandra/conf cassandra/default_conf"
    sh "ln -nfs #{Dir.pwd}/config cassandra/conf"
  end
  
  desc "Start Cassandra"
  task :start do
    raise "Cassandra already running" if cassandra_running?
    Rake::Task["cassandra:setup"].execute unless File.exists?(Dir.pwd + "/cassandra")
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
  sh "java -Xmx512M -jar bin/sbt-launcher.jar #{c}"
end

desc "Compile Scalandra"
task :compile do
  Rake::Task["sbt"].execute("compile")
end

desc "Test scalandra"
task :test do
  Rake::Task["cassandra:start"] unless cassandra_running?
  Rake::Task["sbt"].execute("test")
end