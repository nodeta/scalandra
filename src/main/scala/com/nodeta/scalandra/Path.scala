package com.nodeta.scalandra

/*
 * Common methods for all paths
 */
trait Path {
  def columnFamily() : String
  def key() : String
}

/**
 * This class represents path to a column container, i.e. a row or super column
 * in cassandra.
 * 
 * @author Ville Lautanala
 * @param columnFamily Column family name
 * @param key Row key
 * @param superColumn Super column key (optional)
 */
case class ColumnParent[A](columnFamily : String, key : String, superColumn : Option[A]) extends Path {
  def serialize(s : Serializer[A]) : ColumnParent[Array[Byte]] = {
    new ColumnParent(columnFamily, key, superColumn.map(s.serialize(_)))
  }
  def +[B](column : B) : ColumnPath[A, B] = {
    new ColumnPath(columnFamily, key, superColumn, column)
  }

  def --() : ColumnParent[A] = {
    new ColumnParent[A](columnFamily, key, None)
  }

  def ++[T](superColumn : T) : ColumnParent[T] = {
    new ColumnParent(columnFamily, key, Some(superColumn))
  }
}


object ColumnParent {
  def apply[A](columnFamily : String, key : String) : ColumnParent[A] = {
    new ColumnParent(columnFamily, key, None)
  }

  def apply[A](columnFamily : String, key : String, superColumn : A) : ColumnParent[A] = {
    new ColumnParent(columnFamily, key, Some(superColumn))
  }
}


/**
 * This class represents path to a single standard column in cassandra.
 *
 * @author Ville Lautanala
 * @param columnFamily Column family name
 * @param key Row key
 * @param superColumn Super column key (optional)
 * @param column Column key
 */
case class ColumnPath[A, B](columnFamily : String, key : String, superColumn : Option[A], column : B) extends Path {
  def this(columnFamily : String, key : String, column : B) = this(columnFamily, key, None, column)
}

object ColumnPath {
  def apply[B](columnFamily : String, key : String, column : B) : ColumnPath[B, B] = apply(columnFamily, key, None, column)
}
