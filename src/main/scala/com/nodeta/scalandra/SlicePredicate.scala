package com.nodeta.scalandra

/**
 * Base interface for all client actions.
 *
 * @author Ville Lautanala
 */
trait SlicePredicate[T] {
  val columns : Collection[T]
  val range : Option[Range[T]]
}
