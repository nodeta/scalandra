package com.nodeta.scalandra

/**
 * Predicate used to restrict results.
 *
 * @author Ville Lautanala
 */
trait SlicePredicate[T] {
  val columns : Iterable[T]
  val range : Option[Range[T]]
}
