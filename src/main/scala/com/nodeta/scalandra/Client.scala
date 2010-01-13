package com.nodeta.scalandra

import serializer.{Serializer, NonSerializer}
import map.{ColumnFamily => Fam, StandardColumnFamily => CF, SuperColumnFamily => SCF}

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
  val keyspace : String,
  val serializer : Serialization[A, B, C],
  val consistency : ConsistencyLevels
) extends client.Base[A, B, C] with client.ReadWrite[A, B, C] with map.Keyspace[A, B, C] {
  def this(c : Connection, keyspace : String, serialization : Serialization[A, B, C]) = {
    this(c, keyspace, serialization, ConsistencyLevels())
  }
  protected val client = this
  protected val _client = connection.client

  case class Path(columnFamily : String) extends scalandra.Path[A, B] {
    protected val serializer = client.serializer
  }
  
  case class ColumnParent(columnFamily : String, superColumn : Option[A]) extends scalandra.ColumnParent[A, B] {
    protected val serializer = client.serializer
  }

  case class ColumnPath(columnFamily : String, superColumn : Option[A], column : B) extends scalandra.ColumnPath[A, B] {
    protected val serializer = client.serializer
  }
}

object Client {
  def apply(connection : Connection, keyspace : String) : Client[Array[Byte], Array[Byte], Array[Byte]] = {
    new Client(connection, keyspace, Serialization(NonSerializer, NonSerializer, NonSerializer), ConsistencyLevels())
  }

  def apply[A, B, C](connection : Connection, keyspace : String, serialization : Serialization[A, B, C]) : Client[A, B, C] = {
    new Client(connection, keyspace, serialization, ConsistencyLevels())
  }
  
  def apply[A, B, C](connection : Connection, keyspace : String, serialization : Serialization[A, B, C], consistency : ConsistencyLevels) : Client[A, B, C] = {
    new Client(connection, keyspace, serialization, consistency)
  }

  def apply[T](connection : Connection, keyspace : String, serializer : Serializer[T]) : Client[T, T, T] = {
    new Client(connection, keyspace, Serialization(serializer, serializer, serializer), ConsistencyLevels())
  }
}
