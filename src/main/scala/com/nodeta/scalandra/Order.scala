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
case object Ascending extends Order {
  implicit def toBoolean() : Boolean = false
}

/**
 * Descending sort order
 */
case object Descending extends Order {
  implicit def toBoolean() : Boolean = true
}
