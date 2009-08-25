package com.nodeta.scalandra.serializer

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
