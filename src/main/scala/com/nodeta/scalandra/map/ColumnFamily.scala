package com.nodeta.scalandra.map

import org.apache.cassandra.service.InvalidRequestException
import java.lang.Exception

class UnsupportedActionException(s : String) extends Exception(s) {}

trait ColumnFamily[A] extends scala.collection.mutable.Map[String, A] {
  protected def columnFamily : String
}

trait BaseColumnFamily[A] extends ColumnFamily[A] { this : Base[_, _] =>
  lazy private val keyCache : Option[List[String]] = {
    try {
      Some(client.keys(columnFamily, None, None))
    } catch {
      case x : InvalidRequestException => {
        println(x)
        None
      }
    }
  }

  def elements = {
    keySet.map(key => {
      key -> build(key)
    }).elements
  }

  def get(key : String) : Option[A] = Some(build(key))

  override def keySet() : Set[String] = {
    keyCache match {
      case Some(keys) => Set() ++ keys
      case none => throw new UnsupportedActionException("Key queries can only be performed an order-preserving partitioner")
    }
  }
  
  def size = keySet.size

  protected def build(key : String) : A
}

trait StandardColumnFamily[A, B] extends BaseColumnFamily[StandardRecord[A, B]] with StandardBase[A, B] {
  protected def build(key : String) = {
    val parent = this
    new StandardRecord[A, B] {
      protected val columnSerializer = parent.columnSerializer
      protected val valueSerializer = parent.valueSerializer

      protected val keyspace = parent.keyspace
      protected val connection = parent.connection

      protected val path = ColumnParent[Any](parent.columnFamily, key)
    }
  }

  def -=(key : String) {
    client.remove(ColumnParent[Any](columnFamily, key))
  }


  def update(key : String, value : StandardRecord[A, B]) {
    client.insertNormal(ColumnParent[Any](columnFamily, key), value)
  }
}

trait SuperColumnFamily[A, B, C] extends BaseColumnFamily[SuperRecord[A, B, C]] with SuperBase[A, B, C] {
  protected def build(key : String) = {
    val parent = this
    new SuperRecord[A, B, C] {
      protected val columnSerializer = parent.columnSerializer
      protected val superColumnSerializer = parent.superColumnSerializer
      protected val valueSerializer = parent.valueSerializer

      protected val keyspace = parent.keyspace
      protected val connection = parent.connection

      protected val path = ColumnParent[A](parent.columnFamily, key)
    }
  }

  def -=(key : String) {
    client.remove(ColumnParent[A](columnFamily, key))
  }

  def update(key : String, value : SuperRecord[A, B, C]) {
    client.insertSuper(ColumnParent[A](columnFamily, key), value)
  }
}
