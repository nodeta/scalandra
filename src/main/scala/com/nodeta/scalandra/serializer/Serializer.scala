package com.nodeta.scalandra.serializer

/**
 * Interface for serializers.
 *
 * These are used to serialize and deserialize data going to and coming from
 * cassandra.
 *
 * @author Ville Lautanala
 */
trait Serializer[T] {
  /**
   * Serialize value to byte array
   */ 
  def serialize(t : T) : Array[Byte]

  /**
   * Deserialize value from byte array
   */
  def deserialize(a : Array[Byte]) : T

  def apply(t : T) : Array[Byte] = serialize(t)
  def unapply(a : Array[Byte]) : Option[T] = Some(deserialize(a))

  /**
   * Empty collection for serializer
   *
   * @return Empty byte array or a value representing smallest
   * possible value
   */
  def empty() : Array[Byte] = "".getBytes

  def << (t : T) : Array[Byte] = serialize(t)
  def >> (t : Array[Byte]) : T = deserialize(t)
}

/**
 * This serializer is used when raw data is handled. It does nothing to data.
 */
object NonSerializer extends Serializer[Array[Byte]] {
  def serialize(a : Array[Byte]) = a
  def deserialize(a : Array[Byte]) = a
}
