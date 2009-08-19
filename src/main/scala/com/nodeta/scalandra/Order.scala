package com.nodeta.scalandra

/**
 * Interface to determine sort order when doind range queries.
 */
trait Order {
  def toBoolean : Boolean
}

/**
 * Ascending sort order
 */
object Ascending extends Order {
  implicit def toBoolean() : Boolean = false
}

/**
 * Descending sort order
 */
object Descending extends Order {
  implicit def toBoolean() : Boolean = true
}
