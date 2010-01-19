package com.nodeta.scalandra.client

import com.nodeta.scalandra.serializer.{Serializer, NonSerializer}

import org.apache.cassandra.{service => cassandra}
import org.apache.cassandra.service.NotFoundException

import java.util.{List => JavaList}
import scala.collection.jcl.{ArrayList, Conversions, Map => JavaMap}
import scala.collection.immutable.ListMap

import scalandra.{ColumnPath, ColumnParent}

/**
 * This mixin contains all read-only actions
 *
 * @author Ville Lautanala
 */
trait Read[A, B, C] { this : Base[A, B, C] =>
  private def convert[T](predicate : SlicePredicate[T], serializer : Serializer[T]) : cassandra.SlicePredicate = {
    val items = predicate.columns match {
      case Nil =>
        if (predicate.range.isDefined) null else Nil
      case columns => columns.map(serializer.serialize(_))
    }
    
    val range = predicate.range match {
      case None => null
      case Some(r) =>
        new cassandra.SliceRange(
          r.start.map(serializer.serialize(_)).getOrElse(new Array[Byte](0)),
          r.finish.map(serializer.serialize(_)).getOrElse(new Array[Byte](0)),
          r.order.toBoolean,
          r.count
        )
    }
    
    new cassandra.SlicePredicate(items, range)
  }
  
  implicit private def convert(s : StandardSlice) : cassandra.SlicePredicate = {
    convert(s, serializer.column)
  }

  implicit private def convert(s : SuperSlice) : cassandra.SlicePredicate = {
    convert(s, serializer.superColumn)
  }

  /**
  * Number of columns with specified column path
  */
  def count(key : String, path : ColumnParent[A, B]) : Int = {
    _client.get_count(keyspace, key, path, consistency.read)
  }

  /**
  * Get description of keyspace columnfamilies
  */
  def describe() : Map[String, Map[String, String]] = {
    implicit def convertMap[T](m : java.util.Map[T, java.util.Map[T, T]]) : Map[T, Map[T, T]] = {
      Map.empty ++ Conversions.convertMap(m).map { case(columnFamily, description) =>
        (columnFamily -> (Map.empty ++ Conversions.convertMap(description)))
      }
    }

    _client.describe_keyspace(keyspace)
  }
  
  def apply(key : String, path : ColumnPath[A, B]) = get(key, path)
  def apply(key : String, path : ColumnParent[A, B]) = get(key, path)
  
  /* Get multiple columns from StandardColumnFamily */
  def get(keys : Iterable[String], path : ColumnPath[A, B]) : Map[String, Option[(B, C)]] = {
    (ListMap() ++ multigetAny(keys, path).map { case(k, v) =>
      (k, getColumn(v))
    })
  }
  
  /* Get multiple super columns from SuperColumnFamily */
  def get(keys : Iterable[String], path : ColumnParent[A, B]) : Map[String, Option[(A , Map[B, C])]] = {
    (ListMap() ++ multigetAny(keys, path).map { case(k, v) =>
      (k, getSuperColumn(v))
    })
  }
  
  private def multigetAny(keys : Iterable[String], path : Path[A, B]) : JavaMap[String, cassandra.ColumnOrSuperColumn]= {
    JavaMap(_client.multiget(keyspace, keys, path, consistency.read))
  }

  /**
   * Get single column
   * @param path Path to column
   */
  def get(key : String, path : ColumnPath[A, B]) : Option[C] = {
    try {
      _client.get(
        keyspace,
        key,
        path,
        consistency.read
      ).column match {
        case null => None
        case x : cassandra.Column => Some(serializer.value.deserialize(x.value))
      }
    } catch {
      case e : NotFoundException => None
    }
  }

  /**
   * Get supercolumn
   * @param path Path to super column
   */
  def get(key : String, path : ColumnParent[A, B]) : Option[Map[B, C]] = {
    try {
      getSuperColumn(_client.get(keyspace, key, path, consistency.read)).map(_._2)
    } catch {
      case e : NotFoundException => None
    }
  }
   
  /**
   * Slice columns
   * @param path Path to record or super column
   * @param predicate Search conditions and limits
  */
  def get(key : String, path : Path[A, B], predicate : StandardSlice) : Map[B, C] = {
    ListMap[B, C](_client.get_slice(
      keyspace,
      key,
      path,
      predicate,
      consistency.read
    ).map(getColumn(_).getOrElse({
      throw new NotFoundException()
    })) : _*)
  }
   
  /**
   * Slice super columns
   * @param path Path to record
   * @param predicate Search conditions and limits
   */
  def get(key : String, path : Path[A, B], predicate : SuperSlice) : Map[A, Map[B, C]] = {
    ListMap(_client.get_slice(
      keyspace,
      key,
      path,
      predicate,
      consistency.read
    ).map(getSuperColumn(_).get) : _*)
  }
  
  /**
   * Slice multiple standard column family records
   */
  def get(keys : Iterable[String], path : Path[A, B], predicate : StandardSlice) : Map[String, Map[B, C]] = {
    val result = _client.multiget_slice(keyspace, keys, path, predicate, consistency.read)
    ListMap() ++ Conversions.convertMap(result).map { case(key, value) =>
      key -> (ListMap() ++ value.map(getColumn(_).get))
    }
  }
  
  /**
   * Slice multiple super column family records
   */
  def get(keys : Iterable[String], path : Path[A, B], predicate : SuperSlice) : Map[String, Map[A, Map[B, C]]] = {
    val result = _client.multiget_slice(keyspace, keys, path, predicate, consistency.read)
    ListMap() ++ Conversions.convertMap(result).map { case(key, value) =>
      key -> (ListMap() ++ value.map(getSuperColumn(_).get))
    }
  }

  /**
  * List keys in single keyspace/columnfamily pair
  */
  def keys(columnFamily : String, start : Option[String], finish : Option[String], count : Int) : List[String] = {
    implicit def optionalString(option : Option[String]) : String = {
      option match {
        case Some(s) => s
        case None => ""
      }
    }
    
    val slice = new cassandra.SlicePredicate(
      null,
      new cassandra.SliceRange("".getBytes("UTF-8"), "".getBytes("UTF-8"), true, 1)
    )
    
    val parent = new cassandra.ColumnParent(columnFamily, null)

    _client.get_range_slice(keyspace, parent, slice, optionalString(start), optionalString(finish), count, consistency.read).map(_.key)
  }
  
  
  /**
   * Get slice range for super column family
   */
  def get(path : Path[A, B], predicate : SuperSlice, start : Option[String], finish : Option[String], count : Int) : Map[String, Map[A, Map[B, C]]] = {
    implicit def optionalString(option : Option[String]) : String = {
      option match {
        case Some(s) => s
        case None => ""
      }
    }

    val result = _client.get_range_slice(keyspace, path, predicate, start, finish, count, consistency.read)
    ListMap(result.map { keySlice =>
      (keySlice.key -> ListMap(keySlice.columns.map(getSuperColumn(_).get) : _*))
    } : _*)
  }
  
  /**
   * Get slice range for standard column family
   */
  def get(path : Path[A, B], predicate : StandardSlice, start : Option[String], finish : Option[String], count : Int) : Map[String, Map[B, C]] = {
    implicit def optionalString(option : Option[String]) : String = {
      option match {
        case Some(s) => s
        case None => ""
      }
    }

    val result = _client.get_range_slice(keyspace, path, predicate, start, finish, count, consistency.read)
    ListMap(result.map { keySlice =>
      (keySlice.key -> ListMap(keySlice.columns.map(getColumn(_).get) : _*))
    } : _*)
  }

  implicit private def resultMap(results : JavaList[cassandra.Column]) : Map[B, C] = {
    val r : List[cassandra.Column] = results // Implicit conversion
    ListMap(r.map(c => (serializer.column.deserialize(c.name) -> serializer.value.deserialize(c.value))).toSeq : _*)
  }

  implicit private def superResultMap(results : JavaList[cassandra.SuperColumn]) : Map[A, Map[B, C]] = {
    val r : List[cassandra.SuperColumn] = results // Implicit conversion
    ListMap(r.map(c => (serializer.superColumn.deserialize(c.name) -> resultMap(c.columns))).toSeq : _*)
  }

  implicit private def getSuperColumn(c : cassandra.ColumnOrSuperColumn) : Option[Pair[A, Map[B, C]]] = {
    c.super_column match {
      case null => None
      case x : cassandra.SuperColumn => Some(serializer.superColumn.deserialize(x.name) -> resultMap(x.columns))
    }
  }

  implicit private def getColumn(c : cassandra.ColumnOrSuperColumn) : Option[Pair[B, C]] = {
    c.column match {
      case null => None
      case x : cassandra.Column => Some(serializer.column.deserialize(x.name) -> serializer.value.deserialize(x.value))
    }
  }

  implicit private def convertList[T](list : JavaList[T]) : List[T] = {
    List[T]() ++ Conversions.convertList[T](list)
  }

  implicit private def convertCollection[T](list : Iterable[T]) : JavaList[T] = {
    if (list eq null) null else
    (new ArrayList() ++ list).underlying
  }
}
