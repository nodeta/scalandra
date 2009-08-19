package com.nodeta.scalandra.pool

/**
 * A type-safe interface for object factories.
 *
 * @author Ville Lautanala
 */
trait Factory[T] {
  def build() : T
  def destroy(t : T) : Unit
  def validate(t : T) : Boolean
  def activate(t : T) : Unit
  def passivate(t : T) : Unit
}
