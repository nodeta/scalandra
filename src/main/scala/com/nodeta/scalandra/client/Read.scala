package com.nodeta.scalandra.client

import com.nodeta.scalandra.serializer.{Serializer, NonSerializer}

import org.apache.cassandra.{service => cassandra}
import org.apache.cassandra.service.NotFoundException

import java.util.{List => JavaList}
import scala.collection.jcl.{ArrayList, Conversions, Map => JavaMap}
import scala.collection.immutable.ListMap

/**
 * This mixin contains all read-only actions
 *
 * @author Ville Lautanala
 */
trait Read[A, B, C] { this : Base[A, B, C] =>
  private class SlicePredicate[T](serializer : Serializer[T]) {
    def apply(items : Collection[T]) : cassandra.SlicePredicate = {
      new cassandra.SlicePredicate(items.map(item => serializer(item)).toList, null)
    }

    def apply(start : Option[T], finish : Option[T], order : Order, count : Int) : cassandra.SlicePredicate = {
      implicit def serialize(o : Option[T]) : Array[Byte] = {
        o.map(serializer(_)).getOrElse(serializer.empty)
      }

      new cassandra.SlicePredicate(
        null,
        new cassandra.SliceRange(start, finish, order.toBoolean, count)
      )
    }
  }

  private object SuperSlice extends SlicePredicate[A](serializer.superColumn) {}
  private object StandardSlice extends SlicePredicate[B](serializer.column) {}

  /**
  * Number of columns with specified column path
  */
  def count(path : ColumnParent[A]) : Int = {
    _client.get_count(keyspace, path.key, getColumnParent(path), consistency.read)
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
  
  def apply(path : ColumnPath[A, B]) = get(path)
  def apply(path : ColumnParent[A]) = get(path)
  
  /* Get multiple columns from StandardColumnFamily */
  def multiget(path : MultiPath[A, B]) : Map[String, Option[(B, C)]] = {
    (ListMap() ++ multigetAny(path).map { case(k, v) =>
      (k, getColumn(v))
    })
  }
  
  /* Get multiple super columns from SuperColumnFamily */
  def multigetSuper(path : MultiPath[A, B]) : Map[String, Option[(A , Map[B, C])]] = {
    (ListMap() ++ multigetAny(path).map { case(k, v) =>
      (k, getSuperColumn(v))
    })
  }
  
  private def multigetAny(path : MultiPath[A, B]) : JavaMap[String, cassandra.ColumnOrSuperColumn]= {
    val p = new cassandra.ColumnPath(
      path.columnFamily,
      path.superColumn.map(serializer.superColumn.serialize(_)).getOrElse(null),
      path.column.map(serializer.column.serialize(_)).getOrElse(null)
    )
    JavaMap(_client.multiget(keyspace, path.keys, p, consistency.read))
  }
  
  /* Get multiple records from StandardColumnFamily */
  def getAll(path : MultiPath[A, B]) : Map[String, Map[B, C]] = {
    ListMap() ++ getAllAny(path).map { case(k, v) =>
      (k, ListMap(v.map(getColumn(_).get) : _*))
    }
  }
  
  /* Get multiple records from SuperColumnFamily */
  def getAllSuper(path : MultiPath[A, B]) : Map[String, Map[A , Map[B, C]]] = {
    ListMap() ++ getAllAny(path).map { case(k, v) =>
      (k, ListMap(v.map(getSuperColumn(_).get) : _*))
    }
  }
  
  private def getAllAny(path : MultiPath[A, B]) : JavaMap[String, JavaList[cassandra.ColumnOrSuperColumn]]= {
    val p = new cassandra.ColumnParent(
      path.columnFamily,
      path.superColumn.map(serializer.superColumn.serialize(_)).getOrElse(null)
    )
    JavaMap(
      _client.multiget_slice(
        keyspace,
        path.keys,
        p,
        new SlicePredicate(NonSerializer)(
          None, None, Ascending, maximumCount),
        consistency.read
      )
    )
  }

  /**
   * Get single column
   * @param path Path to column
   */
   def get(path : ColumnPath[A, B]) : Option[C] = {
     try {
       _client.get(
         keyspace,
         path.key,
         getColumnPath(path),
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
   def get(path : ColumnParent[A]) : Option[Map[B, C]] = {
     val s = path.superColumn.get
     sliceSuper(path--, List(s)).get(s)
   }

  /**
   * Slice columns by name
   *
   * @param path Path to super column or row
   * @param columns Collection of columns which are retrieved
   */
   def slice(path : ColumnParent[A], columns : Collection[B]) : Map[B, C] = {
     ListMap[B, C](_client.get_slice(
       keyspace,
       path.key,
       getColumnParent(path),
       StandardSlice(columns),
       consistency.read
     ).map(getColumn(_).getOrElse({
       throw new NotFoundException()
     })) : _*)
   }


  /**
   * Alias for <code>slice</code>
   */
   def slice(path : ColumnParent[A], start : Option[B], finish : Option[B], order : Order) : Map[B, C] = {
     slice(path, start, finish, order, maximumCount)
   }

   /**
    * Slice columns by start and finish
    *
    * @param path Path to super column or row
    * @param start First value of key range
    * @param finish Last value of key range
    * @param order Ordering of results
    * @param count Number of results to return starting from first result
    */
   def slice(path : ColumnParent[A], start : Option[B], finish : Option[B], order : Order, count : Int) : Map[B, C] = {
     ListMap(_client.get_slice(
       keyspace,
       path.key,
       getColumnParent(path),
       StandardSlice(start, finish, order, count),
       consistency.read
     ).map(getColumn(_).getOrElse({
       throw new NotFoundException()
     })) : _*)
   }


   /**
    * Slice super columns by name
    * 
    * @param path Path to row
    * @param columns Collection of super column keys which are retrieved
    */
   def sliceSuper(path : ColumnParent[A], columns : Collection[A]) : Map[A, Map[B, C]] = {
     ListMap(_client.get_slice(
       keyspace,
       path.key,
       getColumnParent(path),
       SuperSlice(columns),
       consistency.read
     ).map(getSuperColumn(_).get) : _*)
   }

   /**
    * Slice columns by start and finish
    *
    * @param path Path to row
    * @param start First value of key range
    * @param finish Last value of key range
    * @param order Ordering of results
    * @param count Number of results to return starting from first result
    */
   def sliceSuper(path : ColumnParent[A], start : Option[A], finish : Option[A], order : Order) : Map[A, Map[B, C]] = {
     sliceSuper(path, start, finish, order, maximumCount)
   }

   /**
    * Slice columns by start and finish with count parameter
    */
   def sliceSuper(path : ColumnParent[A], start : Option[A], finish : Option[A], order : Order, count : Int) : Map[A, Map[B, C]] = {
     ListMap(_client.get_slice(
       keyspace,
       path.key,
       getColumnParent(path),
       SuperSlice(start, finish, order, count),
       consistency.read
     ).map(getSuperColumn(_).get) : _*)
   }

  /**
  * Shorthand for <code>keys</code> without count parameter
  */
  def keys(columnFamily : String, start : Option[String], finish : Option[String]) : List[String] = {
    keys(columnFamily, start, finish, 1000)
  }

  /**
  * List keys in single keyspace/columnfamily pair
  */
  def keys(columnFamily : String, start : Option[String], finish : Option[String], count : Int) : List[String] = {
    def optionalString(option : Option[String]) : String = {
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
    (new ArrayList() ++ list).underlying
  }
}
