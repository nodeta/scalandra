package com.nodeta.scalandra

/**
 * Base interface for all client actions.
 *
 * @author Ville Lautanala
 */
case class Range[T](start : Option[T], finish : Option[T], order : Order, count : Int) {}
