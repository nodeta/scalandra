package com.nodeta.scalandra

import serializer.{Serializer, NonSerializer}
import map.{ColumnFamily, SuperColumnFamily, StandardColumnFamily}

import org.apache.cassandra.{service => cassandra}
import java.lang.IllegalArgumentException

/**
 * This class is a lightweight wrapper for thrift. It supports three levels of
 * serialization.
 *
 * @author Ville Lautanala
 * @param connection An open connection to cassandra
 * @param keyspace Keyspace in which all actions are performed
 * @param superColumn Serializer for super column keys
 * @param column Serializer for column keys
 * @param value Serializer for values in cassandra storage model
 * 
 * @see com.nodeta.scalandra.client.Read
 * @see com.nodeta.scalandra.client.Write
 */
class Client[A, B, C](
  val connection : Connection,
  protected val keyspace : String,
  protected val superColumn : Serializer[A],
  protected val column : Serializer[B],
  protected val value : Serializer[C]
) extends client.Base[A, B, C] with client.ReadWrite[A, B, C] {
  val client = connection.client
  
  lazy private val schema = { describe }
  
  def apply(columnFamily : String) : ColumnFamily[_] = {
    schema(columnFamily)("Type") match {
      case "Super" => superColumnFamily(columnFamily)
      case "Standard" => this.columnFamily(columnFamily)
    }
  }
  
  def columnFamily(columnFamily : String) : StandardColumnFamily[B, C] = {
    val _columnFamily = columnFamily
    val parent = this
    
    new StandardColumnFamily[B, C] {
      protected val columnSerializer = column
      protected val valueSerializer = value

      protected val keyspace = parent.keyspace
      protected val columnFamily = _columnFamily
      protected val connection = parent.connection
    }
  }
  
  def superColumnFamily(columnFamily : String) : SuperColumnFamily[A, B, C] = {
    val _columnFamily = columnFamily
    val parent = this
    
    new SuperColumnFamily[A, B, C] {
      protected val superColumnSerializer = superColumn
      protected val columnSerializer = column
      protected val valueSerializer = value

      protected val keyspace = parent.keyspace
      protected val columnFamily = _columnFamily
      protected val connection = parent.connection
    }
  }

  def build[K, V](k : Serializer[K], v : Serializer[V]) = {
    new Client(connection, keyspace, k, k, v)
  }
}

object Client {
  def apply(keyspace : String) : Client[Array[Byte], Array[Byte], Array[Byte]] = {
    new Client(Connection(), keyspace, NonSerializer, NonSerializer, NonSerializer)
  }
  def apply(connection : Connection, keyspace : String) : Client[Array[Byte], Array[Byte], Array[Byte]] = {
    new Client(connection, keyspace, NonSerializer, NonSerializer, NonSerializer)
  }
  
  def apply[T](connection : Connection, keyspace : String, serializer : Serializer[T]) : Client[T, T, T] = {
    new Client(connection, keyspace, serializer, serializer, serializer)
  }
}
