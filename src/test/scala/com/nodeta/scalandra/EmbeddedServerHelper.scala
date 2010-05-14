package com.nodeta.scalandra.tests

import java.io.{File, FileOutputStream, InputStream, OutputStream}

import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner
import org.apache.cassandra.io.util.FileUtils
import org.apache.cassandra.service.EmbeddedCassandraService
import org.apache.thrift.transport.TTransportException

object EmbeddedServerHelper {
  val tmpDir = new java.io.File("tmp")
  var cassandra : EmbeddedCassandraService = null

  /**
   * Set up embedded cassandra instance and spawn it in a new thread.
   */
  def setup() {
    println("Setting up cassandra..")

    configure()
    cleanup()

    // Start embedded cassandra
    cassandra = new EmbeddedCassandraService()
    cassandra.init()

    val t = new Thread(cassandra, "Cassandra")
    t.setDaemon(true)
    t.start()

    println("Cassandra started")
    System.setProperty("cassandra.running", "true")
  }

  // Copy cassandra configuration to temporary directory
  private def configure() {
    if (tmpDir.exists()) FileUtils.deleteDir(tmpDir)
    copy("/storage-conf.xml", tmpDir)
    copy("/log4j.properties", tmpDir)
    System.setProperty("storage-config", tmpDir.getName)
  }

  def cleanup() {
    // Clean up old data
    val cleaner = new CassandraServiceDataCleaner()
    cleaner.prepare()
  }

  private def copy(resource : String, directory : File) {
    if (!directory.exists()) directory.mkdirs()

    val is = getClass.getResourceAsStream(resource)
    val fileName = resource.substring(resource.lastIndexOf("/") + 1)
    
    val file = new File(directory + System.getProperty("file.separator") + fileName)
    val out = new FileOutputStream(file)
    val buf = new Array[Byte](1024)
    var len = is.read(buf)
    while (len > 0) {
      out.write(buf, 0, len)
      len = is.read(buf)
    }
    out.close()
    is.close()
  }
}