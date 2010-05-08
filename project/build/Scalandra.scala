import sbt._

class ScalandraProject(info: ProjectInfo) extends DefaultProject(info) {
  val cassandraVersion = "0.6.1"

  private def cassandraLibUrl(lib : String) : String = {
    "https://svn.apache.org/repos/asf/cassandra/tags/cassandra-" + cassandraVersion + "/lib/" + lib + ".jar"
  }

  val ibiblioRepo = "iBiblio Maven 2 Repository" at "http://www.ibiblio.org/maven2"
  val snapshotRepo = "Scala Tools Snapshosts" at "http://scala-tools.org/repo-snapshots/"
  val commonsPool = "commons-pool" % "commons-pool" % "1.5.4"
  val slf4j = "org.slf4j" % "slf4j-api" % "1.5.11"
  val specs = "org.scala-tools.testing" %% "specs" % "1.6.5-SNAPSHOT" % "test"

  // Cassandra dependencies for embedded test
  val commonsCodec = "commons-codec" % "commons-codec" % "1.4" % "test"
  val commonsCollections = "commons-collections" % "commons-collections" % "3.2.1" % "test"
  val commonsLang = "commons-lang" % "commons-lang" % "2.4" % "test"
  val googleCollections = "com.google.collections" % "google-collections" % "1.0" % "test"

  // These are not available in any maven repo, downloading from cassandra site
  val clhm = "concurrentlinkedhashmap" % "concurrentlinkedhashmap" % cassandraVersion % "test" from cassandraLibUrl("clhm-production")
  val highScaleLib = "high-scale-lib" % "high-scale-lib" % cassandraVersion % "test" from cassandraLibUrl("high-scale-lib")
  val log4j = "org.slf4j" % "slf4j-log4j12" % "1.5.11" % "test"

  //override def defaultExcludes = super.defaultExcludes || "map"
}
