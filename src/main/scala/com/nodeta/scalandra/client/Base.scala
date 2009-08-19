package com.nodeta.scalandra.client

import org.apache.cassandra.{service => cassandra}
import org.apache.cassandra.service.Cassandra
import java.lang.IllegalArgumentException

/**
 * Base interface for all client actions.
 *
 * @author Ville Lautanala
 */
trait Base[A, B, C] {
  protected val client : Cassandra.Client
  protected val keyspace : String

  protected val maximumCount = 2147483647 // 2^31 -1
  protected val consistency : Int = cassandra.ConsistencyLevel.ONE

  protected val superColumn : Serializer[A]
  protected val column : Serializer[B]
  protected val value : Serializer[C]

  class InvalidPathException(reason : String) extends IllegalArgumentException(reason) {}

  protected def getColumnParent(path : ColumnParent[A]) : cassandra.ColumnParent = {
    new cassandra.ColumnParent(path.columnFamily, path.superColumn.map(superColumn.serialize(_)).getOrElse(null))
  }

  protected def getColumnPath(path : ColumnPath[A, B]) : cassandra.ColumnPath = {
    new cassandra.ColumnPath(path.columnFamily, path.superColumn.map(superColumn.serialize(_)).getOrElse(null), column.serialize(path.column))
  }

  protected def getColumnPath(path : ColumnParent[A]) : cassandra.ColumnPath = {
    // SuperColumn must be found
    val s = path.superColumn.map(superColumn.serialize(_)).getOrElse({
      throw new InvalidPathException("Super Column is not defined")
    })
    new cassandra.ColumnPath(path.columnFamily, s, null)
  }
}
