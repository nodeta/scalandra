package com.nodeta.scalandra

import serializer.{Serializer, StringSerializer}

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

  def build[K, V](k : Serializer[K], v : Serializer[V]) = {
    new Client(connection, keyspace, k, k, v)
  }
}

object Client {
  def apply(keyspace : String) : Client[String, String, String] = {
    new Client(Connection(), keyspace, StringSerializer, StringSerializer, StringSerializer)
  }
  def apply(connection : Connection, keyspace : String) : Client[String, String, String] = {
    new Client(connection, keyspace, StringSerializer, StringSerializer, StringSerializer)
  }
}
