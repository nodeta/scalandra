package com.nodeta.scalandra.client

import org.apache.cassandra.service
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
  val writeConsistency = service.ConsistencyLevel.ZERO

  /**
   * Insert or update value of single column
   */
  def update(key : String, path : ColumnPath[A, B], value : C) {
    cassandra.insert(keyspace,key, path.toColumnPath, this.serializer.value.serialize(value), System.currentTimeMillis, writeConsistency)
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

  private def insert(key : String, path : Path[A, B], data : java.util.List[service.ColumnOrSuperColumn]) {
    val mutation = new LinkedHashMap[String, JavaList[service.ColumnOrSuperColumn]]
    mutation(path.columnFamily) = data

    cassandra.batch_insert(keyspace, key, mutation.underlying, writeConsistency)
  }

  /**
   * Insert collection of values in a standard column family/key pair
   */
  def insertNormal(key : String, path : Path[A, B], data : Iterable[Pair[B, C]]) {
    def convert(data : Iterable[Pair[B, C]]) : java.util.List[service.ColumnOrSuperColumn] = {
      (new ArrayList() ++ data.map { case(k, v) =>
        new service.ColumnOrSuperColumn(
          new service.Column(serializer.column.serialize(k), this.serializer.value.serialize(v), System.currentTimeMillis),
          null
        )
      }).underlying
    }
    insert(key, path, convert(data))
  }

  private def convertToColumnList(data : Iterable[Pair[B, C]]) : JavaList[service.Column] = {
    (new ArrayList[service.Column] ++ data.map { case(k, v) =>
      new service.Column(serializer.column.serialize(k), serializer.value.serialize(v), System.currentTimeMillis)
    }).underlying
  }


  /**
   * Insert collection of values in a super column family/key pair
   */
  def insertSuper(key : String, path : Path[A, B], data : Iterable[Pair[A, Iterable[Pair[B, C]]]]) {
    val cfm = new LinkedHashMap[String, JavaList[service.ColumnOrSuperColumn]]

    val list = (new ArrayList() ++ data.map { case(key, value) =>
      new service.ColumnOrSuperColumn(
        null,
        new service.SuperColumn(serializer.superColumn.serialize(key), convertToColumnList(value))
      )
    }).underlying

    insert(key, path, list)
  }

  /**
   * Remove all values from a path
   *
   * @param path Path to be removed
   */
  def remove(key : String, path : Path[A, B]) {
    cassandra.remove(keyspace, key, path.toColumnPath, System.currentTimeMillis, writeConsistency)
  }
}
