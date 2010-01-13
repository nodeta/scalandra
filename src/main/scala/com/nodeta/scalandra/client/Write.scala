package com.nodeta.scalandra.client

import org.apache.cassandra.{service => cassandra}
import java.util.{List => JavaList}
import scala.collection.jcl.{ArrayList, LinkedHashMap}

/**
 * This mixin contains all write-only actions
 *
 * @author Ville Lautanala
 */
trait Write[A, B, C] { this : Base[A, B, C] =>
  /**
   * Consistency level used for writes
   */
  val writeConsistency = cassandra.ConsistencyLevel.ZERO

  /**
   * Insert or update value of single column
   */
  def update(key : String, path : ColumnPath[A, B], value : C) {
    _client.insert(keyspace,key, path.toColumnPath, this.serializer.value.serialize(value), System.currentTimeMillis, writeConsistency)
  }

  def update(key : String, path : ColumnParent[A, B], value : Iterable[Pair[B, C]]) {
    path.superColumn match {
      case Some(sc) => insertSuper(key, path / None, Map(sc -> value))
      case None => insertNormal(key, path, value)
    }
  }

  def update(key : String, path : Path[A, B], data : Iterable[Pair[A, Iterable[Pair[B, C]]]]) {
    insertSuper(key, path, data)
  }

  private def insert(key : String, path : Path[A, B], data : Iterable[cassandra.ColumnOrSuperColumn]) {
    val mutation = new LinkedHashMap[String, JavaList[cassandra.ColumnOrSuperColumn]]
    mutation(path.columnFamily) = (new ArrayList() ++ data).underlying

    _client.batch_insert(keyspace, key, mutation.underlying, writeConsistency)
  }

  /**
   * Insert collection of values in a standard column family/key pair
   */
  def insertNormal(key : String, path : Path[A, B], data : Iterable[Pair[B, C]]) {
    implicit def convert(data : Iterable[Pair[B, C]]) : List[cassandra.ColumnOrSuperColumn] = {
      data.map { case(k, v) =>
        new cassandra.ColumnOrSuperColumn(
          buildColumn(k, v),
          null
        )
      }.toList
    }
    insert(key, path, data)
  }

  private def buildColumn(key : B, value : C) : cassandra.Column = {
    new cassandra.Column(serializer.column.serialize(key), this.serializer.value(value), System.currentTimeMillis)
  }

  /**
   * Insert collection of values in a super column family/key pair
   */
  def insertSuper(key : String, path : Path[A, B], data : Iterable[Pair[A, Iterable[Pair[B, C]]]]) {
    implicit def convertToColumnList(data : Iterable[Pair[B, C]]) : JavaList[cassandra.Column] = {
      (new ArrayList[cassandra.Column] ++ data.map { case(k, v) =>
        new cassandra.Column(serializer.column.serialize(k), serializer.value(v), System.currentTimeMillis)
      }).underlying
    }

    val cfm = new LinkedHashMap[String, JavaList[cassandra.ColumnOrSuperColumn]]

    val list = data.map { case(key, value) =>
      new cassandra.ColumnOrSuperColumn(
        null,
        new cassandra.SuperColumn(serializer.superColumn.serialize(key), value)
      )
    }.toList

    insert(key, path, list)
  }

  /**
   * Remove all values from a path
   *
   * @param path Path to be removed
   */
  def remove(key : String, path : Path[A, B]) {
    _client.remove(keyspace, key, path, System.currentTimeMillis, writeConsistency)
  }
}
