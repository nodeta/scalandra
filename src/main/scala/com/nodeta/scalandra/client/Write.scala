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
   * Insert collection of values in a standard column family/key pair
   */
  def insertNormal(path : ColumnParent[_], data : Collection[Pair[B, C]]) {
    val cfm = new LinkedHashMap[String, JavaList[cassandra.Column]]
    cfm(path.columnFamily) = data

    val mutation = new cassandra.BatchMutation(path.key, cfm.underlying)

    client.batch_insert(keyspace, mutation, writeConsistency)
  }

  /**
   * Insert collection of values in a super column family/key pair
   */
  def insertSuper(path : ColumnParent[_], data : Collection[Pair[A, Collection[Pair[B, C]]]]) {
    val cfm = new LinkedHashMap[String, JavaList[cassandra.SuperColumn]]
    val list = (new ArrayList[cassandra.SuperColumn] ++ data.map { case(key, value) =>
      new cassandra.SuperColumn(superColumn.serialize(key), value)
    })

    cfm(path.columnFamily) = list.underlying

    val mutation = new cassandra.BatchMutationSuper(path.key, cfm.underlying)
    client.batch_insert_super_column(keyspace, mutation, writeConsistency)
  }

  implicit private def convertToColumnList(data : Collection[Pair[B, C]]) : JavaList[cassandra.Column] = {
    (new ArrayList[cassandra.Column] ++ data.map { case(k, v) =>
      new cassandra.Column(column.serialize(k), value(v), System.currentTimeMillis)
    }).underlying
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
