package com.nodeta.scalandra.pool

import org.apache.commons.pool._
import org.apache.commons.pool.impl._

/**
 * This class wraps Apache ObjectPool to a type-safe scala interface
 *
 * @author Ville Lautanala
 */
class PoolWrapper[T](pool : ObjectPool) extends Pool[T] {
  def borrow() : T = pool.borrowObject.asInstanceOf[T]
  def restore(t : T) = pool.returnObject(t)
  def invalidate(t : T) = pool.invalidateObject(t)
  def add() = pool.addObject
  def idle() : Int = pool.getNumIdle
  def active() : Int = pool.getNumActive
  def clear() : Unit = pool.clear
  def close() : Unit = pool.close
}


/**
 * Pool factory. Uses a soft reference based pool implementation
 */
object SoftReferencePool {
  /**
   * Build a new Pool using given factory.
   */
  def apply[T](f : Factory[T]) : PoolWrapper[T] = {
    new PoolWrapper(new SoftReferenceObjectPool(PoolFactoryWrapper(f)))
  }
}

/**
 * Pool factory. Uses a stack based implementation
 */
object StackPool {
  /**
   * Build a new Pool using given factory.
   */
  def apply[T](f : Factory[T]) : PoolWrapper[T] = {
    new PoolWrapper(new StackObjectPool(PoolFactoryWrapper(f)))
  }
}

/**
 * This class wraps scala based interface to apache's PoolableObjectFactory.
 * It is used when instantiating Apache pools.
 * 
 * @author Ville Lautanala
 * @param factory Object factory used to create instances
 */
case class PoolFactoryWrapper[T](factory : Factory[T]) extends PoolableObjectFactory {
  def makeObject : Object = factory.build.asInstanceOf[Object]
  def destroyObject(o : Object) : Unit = factory.destroy(o.asInstanceOf[T])
  def validateObject(o : Object) : Boolean = factory.validate(o.asInstanceOf[T])
  def activateObject(o : Object) : Unit = factory.activate(o.asInstanceOf[T])
  def passivateObject(o : Object) : Unit = factory.passivate(o.asInstanceOf[T])
}
