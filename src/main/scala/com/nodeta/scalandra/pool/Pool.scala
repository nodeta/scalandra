package com.nodeta.scalandra.pool

/**
 * A type-safe interface used for pool implementations.
 *
 * @author Ville Lautanala
 */
trait Pool[T] extends java.io.Closeable {
  def borrow() : T
  def restore(t : T) : Unit
  def invalidate(t : T) : Unit
  def add() : Unit
  def idle() : Int
  def active() : Int
  def clear() : Unit
  
  def apply[R](f : T => R) = {
    val item = this.borrow()
    try {
      f(item)
    } finally {
      this.restore(item)
    }
  }
}
