import sbt._

class ScalandraProject(info: ProjectInfo) extends DefaultProject(info) {
  val ibiblioRepo = "iBiblio Maven 2 Repository" at "http://www.ibiblio.org/maven2"
  val snapshotRepo = "Scala Tools Snapshosts" at "http://scala-tools.org/repo-snapshots/"
  val commonsPool = "commons-pool" % "commons-pool" % "1.5.4"
  val slf4j = "org.slf4j" % "slf4j-simple" % "1.5.11"
  val specs = "org.scala-tools.testing" %% "specs" % "1.6.5-SNAPSHOT" % "test"

  override def defaultExcludes = super.defaultExcludes || "map"
}
