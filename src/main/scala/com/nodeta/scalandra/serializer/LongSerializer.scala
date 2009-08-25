package com.nodeta.scalandra.serializer

/**
 * This serializer serializes 64-bit integers (Long) to byte arrays in
 * Big-Endian order.
 */
object LongSerializer extends Serializer[Long] {
  import java.nio.ByteBuffer

  def serialize(l : Long) = {
    val bytes = new Array[Byte](8)
    ByteBuffer.wrap(bytes).asLongBuffer.put(l)
    bytes
  }

  def deserialize(a : Array[Byte]) = {
    ByteBuffer.wrap(a).asLongBuffer.get
  }
}
