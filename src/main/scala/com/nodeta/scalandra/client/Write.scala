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
  protected val writeConsistency = cassandra.ConsistencyLevel.ZERO

  /**
   * Insert or update value of single column
   */
  def update(path : ColumnPath[A, B], value : C) {
    client.insert(keyspace, path.key, new cassandra.ColumnPath(
      path.columnFamily,
      path.superColumn.map(superColumn.serialize(_)).getOrElse(null),
      column.serialize(path.column)
    ), this.value.serialize(value), System.currentTimeMillis, writeConsistency)
  }

  def update(path : ColumnParent[A], value : Iterable[Pair[B, C]]) {
    path.superColumn match {
      case Some(sc) => insertSuper(path--, Map(sc -> value))
      case None => insertNormal(path, value)
    }
  }

  def update(path : Path, data : Iterable[Pair[A, Iterable[Pair[B, C]]]]) {
    insertSuper(path, data)
  }

  private def insert(path : Path, data : Iterable[cassandra.ColumnOrSuperColumn]) {
    val mutation = new LinkedHashMap[String, JavaList[cassandra.ColumnOrSuperColumn]]
    mutation(path.columnFamily) = (new ArrayList() ++ data).underlying

    client.batch_insert(keyspace, path.key, mutation.underlying, writeConsistency)
  }

  /**
   * Insert collection of values in a standard column family/key pair
   */
  def insertNormal(path : Path, data : Iterable[Pair[B, C]]) {
    implicit def convert(data : Iterable[Pair[B, C]]) : List[cassandra.ColumnOrSuperColumn] = {
      data.map { case(k, v) =>
        new cassandra.ColumnOrSuperColumn(
          buildColumn(k, v),
          null
        )
      }.toList
    }
    insert(path, data)
  }

  private def buildColumn(key : B, value : C) : cassandra.Column = {
    new cassandra.Column(column.serialize(key), this.value(value), System.currentTimeMillis)
  }

  /**
   * Insert collection of values in a super column family/key pair
   */
  def insertSuper(path : Path, data : Iterable[Pair[A, Iterable[Pair[B, C]]]]) {
    implicit def convertToColumnList(data : Iterable[Pair[B, C]]) : JavaList[cassandra.Column] = {
      (new ArrayList[cassandra.Column] ++ data.map { case(k, v) =>
        new cassandra.Column(column.serialize(k), value(v), System.currentTimeMillis)
      }).underlying
    }

    val cfm = new LinkedHashMap[String, JavaList[cassandra.ColumnOrSuperColumn]]

    val list = data.map { case(key, value) =>
      new cassandra.ColumnOrSuperColumn(
        null,
        new cassandra.SuperColumn(superColumn.serialize(key), value)
      )
    }.toList

    insert(path, list)
  }

  private def remove(key : String, path : cassandra.ColumnPath) {
    client.remove(keyspace, key, path, System.currentTimeMillis, writeConsistency)
  }

  /**
   * Remove column parent and all its columns
   *
   * @param path Path to be removed
   */
  def remove(path : ColumnParent[A]) {
    def convertPath(c : ColumnParent[A]) : cassandra.ColumnPath = {
      new cassandra.ColumnPath(
        c.columnFamily,
        c.superColumn.map(superColumn.serialize(_)).getOrElse(null),
        null
      )
    }

    remove(path.key, convertPath(path))
  }

  /**
   * Remove a single column value
   *
   * @param path Path to column to be removed
   */
  def remove(path : ColumnPath[A, B]) {
    def convertPath(c : ColumnPath[A, B]) : cassandra.ColumnPath = {
      new cassandra.ColumnPath(
        c.columnFamily,
        c.superColumn.map(superColumn.serialize(_)).getOrElse(null),
        column.serialize(c.column)
      )
    }

    remove(path.key, convertPath(path))
  }
}
