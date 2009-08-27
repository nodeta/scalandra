package com.nodeta.scalandra.map

import scala.collection.immutable.ListMap
import scala.collection.mutable.{Map => MMap}

trait Record[A, B] extends MMap[A, B] {
  protected val data : Map[A, B]
  protected val path : ColumnParent[_]

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


trait StandardRecord[A, B] extends Record[A, B] with StandardBase[A, B] {
  protected val path : ColumnParent[Any]

  lazy protected val data : Map[A, B] = {
    client.slice(path, None, None, Ascending)
  }

  def slice(columns : Collection[A]) = {
    client.slice(path--, columns)
  }
  def slice(start : Option[A], finish : Option[A]) = {
    client.slice(path--, start, finish, Ascending)
  } 
  
  protected def build(key : A) = {
    client.get(path--() + key)
  }
  
  def -=(key : A) {
    client.remove(path + key)
  }
  
  override def ++=(kvs : Iterable[(A, B)]) {
    client.insertNormal(path--, kvs.toList)
  }
  
  override def ++(kvs : Iterable[(A, B)]) = {
    this ++= kvs
    this
  }
  
  override def ++=(kvs : Iterator[(A, B)]) {
    this ++ kvs.toList
  }
  
  override def ++(kvs : Iterator[(A, B)]) = {
    this ++= kvs
    this
  }
  
  def update(key : A, value : B) {
    this ++ Map(key -> value)
  }
}


trait SuperRecord[A, B, C] extends Record[A, SuperColumn[A, B, C]] with SuperBase[A, B, C] {
  protected val path : ColumnParent[A]
  lazy protected val data : Map[A, SuperColumn[A, B, C]] = {
    client.sliceSuper(path, None, None, Ascending).transform(buildCached(_, _))
  }

  override def apply(key : A) = build(key).get

  def slice(columns : Collection[A]) : Map[A, SuperColumn[A, B, C]] = {
    client.sliceSuper(path, columns).transform(buildCached(_, _))
  }
  def slice(start : Option[A], finish : Option[A]) : Map[A, SuperColumn[A, B, C]] = {
    client.sliceSuper(path, start, finish, Ascending).transform(buildCached(_, _))
  }
  
  def -=(key : A) {
    client.remove(path ++ key)
  }
  
  override def ++(kvs : Iterable[(A, SuperColumn[A, B, C])]) = {
    client.insertSuper(path--, kvs.toList)
    this
  }
  
  override def ++(kvs : Iterator[(A, SuperColumn[A, B, C])]) = {
    this ++ kvs.toList
  }

  def update(key : A, value : SuperColumn[A, B, C]) {
    client.insertSuper(path--, Map(key -> value))
  }
  
  def update(key : A, value : Map[B, C]) {
    client.insertSuper(path--, Map(key -> value))
  }

  private def buildCached(column : A, _data : Map[B, C]) : SuperColumn[A, B, C] = {
    val parent = this
    new CachedSuperColumn[A, B, C] {
      override lazy protected val data = _data
      protected val columnSerializer = parent.columnSerializer
      protected val superColumnSerializer = parent.superColumnSerializer
      protected val valueSerializer = parent.valueSerializer

      protected val keyspace = parent.keyspace
      protected val connection = parent.connection

      protected val path = (parent.path ++ column)
    }
  }

  protected def build(column : A) = {
    val parent = this
    Some(new SuperColumn[A, B, C] {
      protected val columnSerializer = parent.columnSerializer
      protected val superColumnSerializer = parent.superColumnSerializer
      protected val valueSerializer = parent.valueSerializer

      protected val keyspace = parent.keyspace
      protected val connection = parent.connection

      protected val path = (parent.path ++ column)
    })
  }
}


trait SuperColumn[A, B, C] extends Record[B, C] with SuperBase[A, B, C] {
  protected val path : ColumnParent[A]
  lazy protected val data : Map[B, C] = {
    client.get(path).get
  }

  def slice(columns : Collection[B]) : Map[B, C] = {
    client.slice(path, columns)
  }
  def slice(start : Option[B], finish : Option[B]) : Map[B, C] = {
    client.slice(path, start, finish, Ascending)
  }
  
  def update(key : B, value : C) {
    this ++ Map(key -> value)
  }
  
  override def ++=(kvs : Iterable[(B, C)]) {
    client.insertSuper(path--, Map(path.superColumn.get -> kvs.toList))
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
  def -=(key : B) {
    client.remove(path + key)
  }

  protected def build(column : B) = {
    client.get(path + column)
  }
}

trait CachedSuperColumn[A, B, C] extends SuperColumn[A, B, C] {
  override protected def build(column : B) = {
    data.get(column)
  }
}
