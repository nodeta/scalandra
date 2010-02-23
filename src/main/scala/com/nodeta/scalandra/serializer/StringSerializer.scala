package com.nodeta.scalandra.serializer

/**
 * This serializer handles UTF-8 encoded strings
 */
object StringSerializer extends Serializer[String] {
  def serialize(s : String) = {
    if (s ne null)
      s.getBytes("UTF-8")
    else
      empty
  }
  def deserialize(a : Array[Byte]) = new String(a, "UTF-8")
}
