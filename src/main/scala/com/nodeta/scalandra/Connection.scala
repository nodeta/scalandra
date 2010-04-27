package com.nodeta.scalandra

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.apache.cassandra.thrift.Cassandra

import java.io.{Closeable, Flushable}


/**
 * Wrapper for cassandra socket connection. Automatically opens a connection
 * when an object is instantiated.
 *
 * @author Ville Lautanala
 * @param host Hostname or IP address of Cassandra server
 * @param port Port to connect
 */
class Connection(host : String, port : Int, timeout : Int) extends Closeable with Flushable {
  def this() = this("127.0.0.1", 9160, 0)
  def this(host : String) = this(host, 9160, 0)
  def this(port : Int) = this("127.0.0.1", port, 0)
  def this(host : String, port : Int) = this(host, port, 0)

  private val socket = new TSocket(host, port, timeout)

  /**
   * Unwrapped cassandra client
   */
  val client = new Cassandra.Client(new TBinaryProtocol(socket))
  socket.open()
  
  /**
   * (Re-)Open socket to cassandra if connection is not already opened
   */
  def open() {
    if (!isOpen) socket.open()
  }

  /**
   * Close connection to cassandra
   */
  def close() {
    socket.close()
  }
  
  /**
   * Check connection status
   */
  def isOpen() : Boolean = {
    socket.isOpen()
  }

  def flush() {
    socket.flush()
  }
}

object Connection {
  def apply() : Connection = new Connection()
  def apply(host : String) : Connection = new Connection(host)
  def apply(port : Int) : Connection = new Connection(port)
  def apply(host : String, port : Int) : Connection = new Connection(host, port)
  def apply(host : String, port : Int, timeout : Int) : Connection = new Connection(host, port, timeout)
}
