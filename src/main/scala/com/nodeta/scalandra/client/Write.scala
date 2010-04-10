package com.nodeta.scalandra.client

import com.nodeta.scalandra._
import org.apache.cassandra.thrift
import org.apache.cassandra.thrift.ThriftGlue
import java.util.{List => JavaList}
import scala.collection.JavaConversions
import scala.collection.mutable.{Buffer, Map => MutableMap}
/**
 * This mixin contains all write-only actions
 *
 * @author Ville Lautanala
 */
trait Write[A, B, C] { this : Base[A, B, C] =>
  /**
   * Insert or update value of single column
   */
  def update(key : String, path : ColumnPath[A, B], value : C) {
    cassandra.insert(keyspace,key, path.toColumnPath, this.serializer.value.serialize(value), System.currentTimeMillis, consistency.write)
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

  private def insert(key : String, path : Path[A, B], data : java.util.List[thrift.ColumnOrSuperColumn]) {
    val mutation = JavaConversions.asMap(MutableMap(path.columnFamily -> data))
    cassandra.batch_insert(keyspace, key, mutation, consistency.write)
  }

  /**
   * Insert collection of values in a standard column family/key pair
   */
  def insertNormal(key : String, path : Path[A, B], data : Iterable[Pair[B, C]]) {
    def convert(data : Iterable[Pair[B, C]]) : java.util.List[thrift.ColumnOrSuperColumn] = {
      val buffer = Buffer[thrift.ColumnOrSuperColumn]()
      buffer ++= data.map { case(k, v) =>
        ThriftGlue.createColumnOrSuperColumn_Column(
          new thrift.Column(serializer.column.serialize(k), this.serializer.value.serialize(v), System.currentTimeMillis))
      }
      JavaConversions.asList(buffer)
    }
    insert(key, path, convert(data))
  }

  private def convertToColumnList(data : Iterable[Pair[B, C]]) : JavaList[thrift.Column] = {
    val buffer = Buffer[thrift.Column]()
    buffer ++= data.map { case(k, v) =>
      new thrift.Column(serializer.column.serialize(k), serializer.value.serialize(v), System.currentTimeMillis)
    }
    JavaConversions.asList(buffer)
  }


  /**
   * Insert collection of values in a super column family/key pair
   */
  def insertSuper(key : String, path : Path[A, B], data : Iterable[Pair[A, Iterable[Pair[B, C]]]]) {
    val buffer = Buffer[thrift.ColumnOrSuperColumn]()
    buffer ++= data.map { case(key, value) =>
      ThriftGlue.createColumnOrSuperColumn_SuperColumn(
        new thrift.SuperColumn(serializer.superColumn.serialize(key), convertToColumnList(value))
      )
    }

    insert(key, path, JavaConversions.asList(buffer))
  }

  /**
   * Remove all values from a path
   *
   * @param path Path to be removed
   */
  def remove(key : String, path : Path[A, B]) {
    cassandra.remove(keyspace, key, path.toColumnPath, System.currentTimeMillis, consistency.write)
  }
}
