package com.nodeta.scalandra

/**
 * Base interface for all client actions.
 *
 * @author Ville Lautanala
 */
trait SlicePredicate[T] {
  val columns : Iterable[T]
  val range : Option[Range[T]]
}
