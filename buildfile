require 'buildr/scala'
require 'buildr/java/commands'

# Version number for this release
VERSION_NUMBER = "0.1.0"
# Group identifier for your projects
GROUP = "com.nodeta"
COPYRIGHT = "Nodeta Oy"

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://www.ibiblio.org/maven2"
repositories.remote << 'http://maven.kutomotie.nodeta.fi'


desc "The Scalandra project"
define "scalandra" do
  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT
  
  doc.using :scaladoc
  test.using :specs
  
  compile.with(
    'org.apache.cassandra:cassandra:jar:0.4.0-beta1',
    'org.apache.thrift:thrift:jar:cassandra-0.4.0-beta1',
    'commons-pool:commons-pool:jar:1.5.2',
    'log4j:log4j:jar:1.2.15'
  ).using(:deprecation => true)
end
