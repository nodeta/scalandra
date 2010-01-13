package com.nodeta.scalandra.map

import serializer.Serializer

trait Base[A, B ,C] {
  protected val client : Client[A, B, C]
}
