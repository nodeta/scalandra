package com.nodeta.scalandra.map

import scala.collection.immutable.ListMap
import scala.collection.mutable.{Map => MMap}

trait Record[A, B] extends CassandraMap[A, B] {
  val key : String
  val path : Path[_, _]
  
  def size = {
    elements.toList.size
  }
}

class StandardRecord[A, B, C](val key : String, val path : ColumnParent[A, B], protected val client : Client[A, B, C]) extends Record[B, C] with Base[A, B, C] {
  lazy private val defaultRange = {
    Range[B](None, None, Ascending, 2147483647)
  }
  
  sealed protected trait ListPredicate extends StandardRecord[A, B, C] {
    def constraint : Iterable[B]
    override def elements = {
      this.client.get(this.key, this.path, this.client.StandardSlice(constraint)).elements
    }
  }
  
  sealed protected trait RangePredicate extends StandardRecord[A, B, C] {
    def constraint : Range[B]
    override def elements = {
      this.client.get(this.key, this.path, this.client.StandardSlice(this.constraint)).elements
    }
  }
  
  def elements = {
    client.get(key, path, client.StandardSlice(defaultRange)).elements
  }
  
  def get(column : B) : Option[C] = {
    client.get(key, path / column)
  }
  
  def slice(r : Range[B]) = {
    new StandardRecord(key, path, client) with RangePredicate {
      val constraint = r
    }
  }
  
  def slice(r : Iterable[B]) = {
    new StandardRecord(key, path, client) with ListPredicate {
      val constraint = r
    }
  }
  
  def remove(column : B) = {
    client.remove(key, path / column)
    this
  }
  
  def update(column : B, value : C) = {
    client(key, path / column) = value
    this
  }
}

class SuperRecord[A, B, C](val key : String, val path : Path[A, B], protected val client : Client[A, B, C]) extends Record[A, scala.collection.Map[B, C]] with Base[A, B, C] {
  lazy private val defaultRange = {
    Range[A](None, None, Ascending, 2147483647)
  }
  
  sealed protected trait ListPredicate extends SuperRecord[A, B, C] {
    def constraint : Iterable[A]
    override def elements = {
      this.client.get(this.key, this.path, this.client.SuperSlice(constraint)).elements
    }
  }
  
  sealed protected trait RangePredicate extends SuperRecord[A, B, C] {
    def constraint : Range[A]
    override def elements = {
      this.client.get(this.key, this.path, this.client.SuperSlice(this.constraint)).elements
    }
  }
  
  def elements = {
    client.get(key, path, client.SuperSlice(defaultRange)).elements
  }
  
  def get(column : A) : Option[scala.collection.Map[B, C]] = {
    Some(new StandardRecord(key, path / Some(column), client))
  }
  
  def slice(r : Range[A]) = {
    new SuperRecord(key, path, client) with RangePredicate {
      val constraint = r
    }
  }
  
  def slice(r : Iterable[A]) = {
    new SuperRecord(key, path, client) with ListPredicate {
      val constraint = r
    }
  }
  
  def remove(column : A) = {
    client.remove(key, path / Some(column))
    this
  }
  
  def update(column : A, value : scala.collection.Map[B, C]) = {
    updated(column, value)
    this
  }
  
  def update(column : A, value : Iterable[(B, C)]) = {
    updated(column, value)
    this
  }
  
  private def updated(column : A, value : Iterable[(B, C)]) {
    client(key, path / Some(column)) = value
  }
}
