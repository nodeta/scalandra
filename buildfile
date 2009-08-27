require 'buildr/scala'

# Version number for this release
VERSION_NUMBER = "0.1.0"
# Group identifier for your projects
GROUP = "com.nodeta"
COPYRIGHT = "Nodeta Oy"

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://www.ibiblio.org/maven2"

desc "Scalandra"
define "scalandra" do
  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT
  
  doc.using :vscaladoc
  test.using :specs
  
  compile.with(
    'libs/cassandra-0.4.0-beta1.jar',
    'libs/thrift-cassandra-0.4.0-beta1.jar',
    'commons-pool:commons-pool:jar:1.5.2',
    transitive('commons-logging:commons-logging:jar:1.1')
  ).using(:deprecation => true)
  
  package :jar
end
