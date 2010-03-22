package com.nodeta.scalandra

/**
 * Range class used in range based limiting
 *
 * @author Ville Lautanala
 */
case class Range[T](start : Option[T], finish : Option[T], order : Order, count : Int) {}
