package com.nodeta.scalandra

import org.apache.cassandra.{service => cassandra}

trait Path[A, B] {
  protected def serializer : Serialization[A, B, _]
  def columnFamily : String
  def /(_sC : Option[A]) = {
    val parent = this
    new ColumnParent[A, B] {
      protected def serializer = parent.serializer
      def superColumn = _sC
      def columnFamily = parent.columnFamily
    }
  }
  
  def toColumnParent : cassandra.ColumnParent = {
    new cassandra.ColumnParent(columnFamily, null)
  }
  
  def toColumnPath : cassandra.ColumnPath = {
    new cassandra.ColumnPath(columnFamily, null, null)
  }
}

trait ColumnParent[A, B] extends Path[A, B] {
  def superColumn : Option[A]
  
  def /(c : B) : ColumnPath[A, B] = {
    val parent = this
    new ColumnPath[A, B] {
      protected def serializer = parent.serializer
      def columnFamily = parent.columnFamily
      def superColumn = parent.superColumn
      val column = c
    }
  }
  
  lazy protected val _superColumn : Array[Byte] = {
    superColumn.map(serializer.superColumn.serialize(_)).getOrElse(null)
  }
  
  override def toColumnParent : cassandra.ColumnParent = {
    new cassandra.ColumnParent(columnFamily, _superColumn)
  }
  
  override def toColumnPath : cassandra.ColumnPath = {
    new cassandra.ColumnPath(columnFamily, _superColumn, null)
  }
}

trait ColumnPath[A, B] extends ColumnParent[A, B] {
  def column : B
  
  lazy protected val _column : Array[Byte] = {
    serializer.column.serialize(column)
  }
  
  override def toColumnPath : cassandra.ColumnPath = {
    new cassandra.ColumnPath(columnFamily, _superColumn, _column)
  }  
}
