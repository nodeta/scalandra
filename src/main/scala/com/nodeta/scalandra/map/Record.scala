package com.nodeta.scalandra.map

import scala.collection.immutable.ListMap
import scala.collection.mutable.{Map => MMap}

trait Record[A, B] extends MMap[A, B] {
  protected val data : Map[A, B]
  protected val columnFamily : String
  protected val key : String

  protected def build(key : A) : Option[B]
  def elements = data.elements
  def get(key : A) = data.get(key)
  def size = data.size
  
  def slice(columns : Collection[A]) : Map[A, B]
  def slice(start : Option[A], finish : Option[A]) : Map[A, B]
  def slice(start : A, finish : A) : Map[A, B] = {
    slice(Some(start), Some(finish))
  }
}


trait StandardRecord[A, B, C] extends Record[B, C] with Base[A, B, C] {
  lazy private val path = { 
    client.ColumnParent(columnFamily, None)
  }
  lazy protected val data : Map[B, C] = {
    client.slice(key, path, None, None, Ascending)
  }

  def slice(columns : Collection[B]) = {
    client.slice(key, path, columns)
  }
  def slice(start : Option[B], finish : Option[B]) = {
    client.slice(key, path, start, finish, Ascending)
  } 
  
  protected def build(column : B) = {
    client.get(key, path / column)
  }
  
  def -=(column : B) {
    client.remove(key, path / column)
  }
  
  override def ++=(kvs : Iterable[(B, C)]) {
    client.insertNormal(key, path, kvs.toList)
  }
  
  override def ++(kvs : Iterable[(B, C)]) = {
    this ++= kvs
    this
  }
  
  override def ++=(kvs : Iterator[(B, C)]) {
    this ++ kvs.toList
  }
  
  override def ++(kvs : Iterator[(B, C)]) = {
    this ++= kvs
    this
  }
  
  def update(key : B, value : C) {
    this ++ Map(key -> value)
  }
}


trait SuperRecord[A, B, C] extends Record[A, SuperColumn[A, B, C]] with Base[A, B, C] {
  lazy private val path = client.ColumnParent(columnFamily, None)
  lazy protected val data : Map[A, SuperColumn[A, B, C]] = {
    client.sliceSuper(key, path, None, None, Ascending).transform(buildCached(_, _))
  }

  override def apply(key : A) = build(key).get

  def slice(columns : Collection[A]) : Map[A, SuperColumn[A, B, C]] = {
    client.sliceSuper(key, path, columns).transform(buildCached(_, _))
  }

  def slice(start : Option[A], finish : Option[A]) : Map[A, SuperColumn[A, B, C]] = {
    client.sliceSuper(key, path, start, finish, Ascending).transform(buildCached(_, _))
  }

  def slice(start : Option[A], finish : Option[A], order : Order, count : Int) : Map[A, SuperColumn[A, B, C]] = {
    client.sliceSuper(key, path, start, finish, order, count).transform(buildCached(_, _))
  }

  def -=(superColumn : A) {
    client.remove(key, path / Some(superColumn))
  }
  
  override def ++(kvs : Iterable[(A, SuperColumn[A, B, C])]) = {
    client.insertSuper(key, path, kvs.toList)
    this
  }
  
  override def ++(kvs : Iterator[(A, SuperColumn[A, B, C])]) = {
    this ++ kvs.toList
  }

  def update(column : A, value : SuperColumn[A, B, C]) {
    client.insertSuper(key, path, Map(column -> value))
  }
  
  def update(column : A, value : Map[B, C]) {
    client.insertSuper(key, path, Map(column -> value))
  }

  private def buildCached(column : A, _data : Map[B, C]) : SuperColumn[A, B, C] = {
    val parent = this
    new CachedSuperColumn[A, B, C] {
      override lazy protected val data = _data
      protected val client = parent.client
      protected val columnFamily = parent.columnFamily
      protected val key = parent.key

      protected val path = (parent.path / Some(column))
    }
  }

  protected def build(column : A) = {
    val parent = this
    Some(new SuperColumn[A, B, C] {
      protected val client = parent.client
      protected val columnFamily = parent.columnFamily
      protected val path = (parent.path / Some(column))
      protected val key = parent.key
    })
  }
}


trait SuperColumn[A, B, C] extends Record[B, C] with Base[A, B, C] {
  
  
  protected val path : ColumnParent[A, B]
  lazy protected val data : Map[B, C] = {
    client.get(key, path).get
  }

  def slice(columns : Collection[B]) : Map[B, C] = {
    client.slice(key, path, columns)
  }
  def slice(start : Option[B], finish : Option[B]) : Map[B, C] = {
    client.slice(key, path, start, finish, Ascending)
  }
  
  def update(key : B, value : C) {
    this ++ Map(key -> value)
  }
  
  override def ++=(kvs : Iterable[(B, C)]) {
    client.insertSuper(key, path, Map(path.superColumn.get -> kvs.toList))
  }
  
  override def ++(kvs : Iterable[(B, C)]) = {
    this ++= kvs
    this
  }
  
  override def ++=(kvs : Iterator[(B, C)]) {
    this ++ kvs.toList
  }
  
  override def ++(kvs : Iterator[(B, C)]) = {
    this ++= kvs
    this
  }
  def -=(column : B) {
    client.remove(key, path / column)
  }

  protected def build(column : B) = {
    client.get(key, path / column)
  }
}

trait CachedSuperColumn[A, B, C] extends SuperColumn[A, B, C] {
  override protected def build(column : B) = {
    data.get(column)
  }
}
