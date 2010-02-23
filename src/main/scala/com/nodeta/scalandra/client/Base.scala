package com.nodeta.scalandra.client

import org.apache.cassandra.{service => cassandra}
import org.apache.cassandra.service.Cassandra
import java.lang.IllegalArgumentException
import serializer.Serializer

/**
 * Base interface for all client actions.
 *
 * @author Ville Lautanala
 */
trait Base[A, B, C] {
  private val self = this
  protected val _client : Cassandra.Client
  protected val keyspace : String

  protected val maximumCount = 2147483647 // 2^31 -1
  def consistency : ConsistencyLevels

  protected val serializer : Serialization[A, B, C]

  class InvalidPathException(reason : String) extends IllegalArgumentException(reason) {}

  case class StandardSlice(columns : Iterable[B], range : Option[Range[B]]) extends SlicePredicate[B] {
    def this(columns : Iterable[B]) = this(columns, None)
    def this(range : Range[B]) = this(Nil, Some(range))
  }
  
  object StandardSlice {
    def apply(columns : Iterable[B]) : StandardSlice = apply(columns, None)
    def apply(range : Range[B]) : StandardSlice = apply(Nil, Some(range))
  }

  case class SuperSlice(columns : Iterable[A], range : Option[Range[A]]) extends SlicePredicate[A] {
    def this(columns : Iterable[A]) = this(columns, None)
    def this(range : Range[A]) = this(Nil, Some(range))
  }
  
  object SuperSlice {
    def apply(columns : Iterable[A]) : SuperSlice = apply(columns, None)
    def apply(range : Range[A]) : SuperSlice = apply(Nil, Some(range))
  }

}
