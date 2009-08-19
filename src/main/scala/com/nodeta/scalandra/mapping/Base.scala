package com.nodeta.scalandra.mapping

trait Base[A, B] {
  protected val columnSerializer : Serializer[A]
  protected val valueSerializer : Serializer[B]

  protected val keyspace : String
  protected val connection : Connection

  protected val client : Client[_, A, B]
}

trait StandardBase[A, B] extends Base[A, B] {
  lazy protected val client : Client[Any, A, B] = {
    new Client(connection, keyspace, DummySerializer, columnSerializer, valueSerializer)
  }
}

trait SuperBase[A, B, C] extends Base[B, C] {
  protected val superColumnSerializer : Serializer[A]

  lazy override protected val client : Client[A, B, C] = {
    new Client(connection, keyspace, superColumnSerializer, columnSerializer, valueSerializer)
  }
}
