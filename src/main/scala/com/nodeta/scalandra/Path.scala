package com.nodeta.scalandra

/*
 * Common methods for all paths
 */
case class Path(columnFamily : String, key : String) {}

/**
 * This class represents path to a column container, i.e. a row or super column
 * in cassandra.
 * 
 * @author Ville Lautanala
 * @param columnFamily Column family name
 * @param key Row key
 * @param superColumn Super column key (optional)
 */
case class ColumnParent[A](override val columnFamily : String, override val key : String, superColumn : Option[A]) extends Path(columnFamily, key) {
  def +[B](column : B) : ColumnPath[A, B] = {
    new ColumnPath(columnFamily, key, superColumn, column)
  }

  def --() : ColumnParent[A] = {
    new ColumnParent[A](columnFamily, key, None)
  }
  
  def -() : Path = {
    Path(columnFamily, key)
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
case class ColumnPath[A, B](override val columnFamily : String, override val key : String, superColumn : Option[A], column : B) extends Path(columnFamily, key) {
  def this(columnFamily : String, key : String, column : B) = this(columnFamily, key, None, column)
}

object ColumnPath {
  def apply[B](columnFamily : String, key : String, column : B) : ColumnPath[B, B] = apply(columnFamily, key, None, column)
}

case class MultiPath[A, B](columnFamily : String, keys : Iterable[String], superColumn : Option[A], column : Option[B]) {
  def this(columnFamily : String, key : Iterable[String]) = this(columnFamily, key, None, None)
}

object MultiPath {
  def apply[A, B](columnFamily : String, keys : Iterable[String]) : MultiPath[A, B] = this(columnFamily, keys, None, None)
}