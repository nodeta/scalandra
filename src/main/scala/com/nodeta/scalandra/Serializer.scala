package com.nodeta.scalandra

import java.nio.ByteBuffer

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
 * Do-nothing serializer
 */
object DummySerializer extends Serializer[Any] {
  def serialize(a : Any) = empty
  def deserialize(a : Array[Byte]) = null
}

/**
 * This serializer is used when raw data is handled. It does nothing to data.
 */
object NonSerializer extends Serializer[Array[Byte]] {
  def serialize(a : Array[Byte]) = a
  def deserialize(a : Array[Byte]) = a
}

/**
 * This serializer handles UTF-8 encoded strings
 */
object StringSerializer extends Serializer[String] {
  def serialize(s : String) = {
    s match {
      case null => "".getBytes
      case s =>s.getBytes("UTF-8")
    }
  }
  def deserialize(a : Array[Byte]) = new String(a, "UTF-8")
}

/**
 * This serializer serializes 64-bit integers (Long) to byte arrays in
 * Big-Endian order.
 */
object LongSerializer extends Serializer[Long] {
  def serialize(l : Long) = {
    val bytes = new Array[Byte](8)
    ByteBuffer.wrap(bytes).asLongBuffer.put(l)
    bytes
  }

  def deserialize(a : Array[Byte]) = {
    ByteBuffer.wrap(a).asLongBuffer.get
  }
}
