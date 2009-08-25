package com.nodeta.scalandra.map

import scala.collection.immutable.ListMap

trait Row[A, B] extends scala.collection.Map[A, B] {
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


trait StandardRow[A, B] extends Row[A, B] with StandardBase[A, B] {
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
}


trait SuperRow[A, B, C] extends Row[A, SuperColumnRow[A, B, C]] with SuperBase[A, B, C] {
  protected val path : ColumnParent[A]
  lazy protected val data : Map[A, SuperColumnRow[A, B, C]] = {
    client.sliceSuper(path, None, None, Ascending).transform(buildCached(_, _))
  }

  override def apply(key : A) = build(key).get

  def slice(columns : Collection[A]) : Map[A, SuperColumnRow[A, B, C]] = {
    client.sliceSuper(path, columns).transform(buildCached(_, _))
  }
  def slice(start : Option[A], finish : Option[A]) : Map[A, SuperColumnRow[A, B, C]] = {
    client.sliceSuper(path, start, finish, Ascending).transform(buildCached(_, _))
  }
  
  private def buildCached(column : A, _data : Map[B, C]) : SuperColumnRow[A, B, C] = {
    val parent = this
    new CachedSuperColumnRow[A, B, C] {
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
    Some(new SuperColumnRow[A, B, C] {
      protected val columnSerializer = parent.columnSerializer
      protected val superColumnSerializer = parent.superColumnSerializer
      protected val valueSerializer = parent.valueSerializer

      protected val keyspace = parent.keyspace
      protected val connection = parent.connection

      protected val path = (parent.path ++ column)
    })
  }
}


trait SuperColumnRow[A, B, C] extends Row[B, C] with SuperBase[A, B, C] {
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

  protected def build(column : B) = {
    client.get(path + column)
  }
}

trait CachedSuperColumnRow[A, B, C] extends SuperColumnRow[A, B, C] {
  override protected def build(column : B) = {
    data.get(column)
  }
}
