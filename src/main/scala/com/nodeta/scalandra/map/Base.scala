package com.nodeta.scalandra.map

import serializer.Serializer

trait Base[B ,C] {
  protected def client : Client[_, B, C]
}

trait StandardBase[A, B] extends Base[A, B] {
  protected def client : Client[Any, A, B]
}

trait SuperBase[A, B, C] extends Base[B, C] {
  protected def client : Client[A, B, C]
}
