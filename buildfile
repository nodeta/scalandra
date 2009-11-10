require 'buildr/scala'

# Version number for this release
VERSION_NUMBER = "0.2.0-dev"
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
  
  #doc.using :vscaladoc
  test.using :specs
  
  compile.with(
    'libs/apache-cassandra-incubating-0.4.1.jar',
    'libs/libthrift-r808609.jar',
    'commons-pool:commons-pool:jar:1.5.2',
    transitive('org.slf4j:slf4j-simple:jar:1.5.8')
  ).using(:deprecation => true)
  
  package :jar
end
