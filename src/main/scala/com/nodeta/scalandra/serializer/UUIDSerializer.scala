package com.nodeta.scalandra.serializer

import java.util.UUID

/**
 * This serializer serializes A UUID to byte arrays in
 * Big-Endian order.
 */
object UUIDSerializer extends Serializer[UUID] {
  import java.nio.ByteBuffer

  def serialize(u : UUID) = {
    val bytes = new Array[Byte](16)
    val longBuffer = ByteBuffer.wrap(bytes).asLongBuffer
    longBuffer.put(u.getMostSignificantBits).put(u.getLeastSignificantBits)
    bytes
  }

  def deserialize(a : Array[Byte]) = {
    val longBuffer = ByteBuffer.wrap(a).asLongBuffer
    new UUID(longBuffer.get, longBuffer.get)
  }
}
