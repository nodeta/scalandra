package com.nodeta.scalandra

import org.apache.cassandra.{thrift => cassandra}
import org.apache.cassandra.thrift.ThriftGlue

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
  
  def /(c : B) : ColumnPath[A, B] = {
    val parent = this
    new ColumnPath[A, B] {
      protected def serializer = parent.serializer
      def columnFamily = parent.columnFamily
      def superColumn = None
      val column = c
    }
  }
  
  def toColumnParent : cassandra.ColumnParent = {
    ThriftGlue.createColumnParent(columnFamily, null)
  }
  
  def toColumnPath : cassandra.ColumnPath = {
    ThriftGlue.createColumnPath(columnFamily, null, null)
  }
  override def toString = {
    "Path(" + columnFamily + ")"
  }
}

trait ColumnParent[A, B] extends Path[A, B] {
  def superColumn : Option[A]
  
  override def /(c : B) : ColumnPath[A, B] = {
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
    ThriftGlue.createColumnParent(columnFamily, _superColumn)
  }
  
  override def toColumnPath : cassandra.ColumnPath = {
    ThriftGlue.createColumnPath(columnFamily, _superColumn, null)
  }
  
  override def toString = {
    "ColumnParent(" + columnFamily + "," + superColumn + ")"
  }
}

trait ColumnPath[A, B] extends ColumnParent[A, B] {
  def column : B
  
  lazy protected val _column : Array[Byte] = {
    serializer.column.serialize(column)
  }
  
  override def toColumnPath : cassandra.ColumnPath = {
    ThriftGlue.createColumnPath(columnFamily, _superColumn, _column)
  }
  
  override def toString = {
    "ColumnPath(" + columnFamily + "," + superColumn + "," + column + ")"
  }
}
