package com.nodeta.scalandra.map

import org.apache.cassandra.service.InvalidRequestException
import java.lang.Exception

class UnsupportedActionException(s : String) extends Exception(s) {}

trait ColumnFamily[A] extends scala.collection.mutable.Map[String, A] {
  protected val columnFamily : String
}

trait BaseColumnFamily[A] extends ColumnFamily[A] { this : Base[_, _, _] =>
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

trait StandardColumnFamily[A, B, C] extends BaseColumnFamily[StandardRecord[A, B, C]] with Base[A, B, C] {
  protected def build(_key : String) = {
    val parent = this
    new StandardRecord[A, B, C] {
      protected val client = parent.client

      protected val columnFamily = parent.columnFamily
      protected val key = _key
    }
  }

  def -=(key : String) {
    client.remove(key, client.Path(columnFamily))
  }


  def update(key : String, value : StandardRecord[A, B, C]) {
    client.insertNormal(key, client.Path(columnFamily), value)
  }
  
  def update(key : String, value : Map[B, C]) {
    //client.insertNormal(key, client.ColumnParent(columnFamily, None), value)
  }
}

trait SuperColumnFamily[A, B, C] extends BaseColumnFamily[SuperRecord[A, B, C]] with Base[A, B, C] {
  protected def build(_key : String) = {
    val parent = this
    new SuperRecord[A, B, C] {
      protected val client = parent.client
      protected val key = _key
      protected val columnFamily = parent.columnFamily
    }
  }

  def -=(key : String) {
    client.remove(key, client.Path(columnFamily))
  }

  def update(key : String, value : SuperRecord[A, B, C]) {
    client.insertSuper(key, client.ColumnParent(columnFamily, None), value)
  }
  
  def update(key : String, value : Map[A, Map[B, C]]) {
    client.insertSuper(key, client.ColumnParent(columnFamily, None), value)
  }
}
