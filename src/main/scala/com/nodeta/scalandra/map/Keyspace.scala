package com.nodeta.scalandra.map

import map.{ColumnFamily => CF, SuperColumnFamily => SCF}


trait Keyspace[A, B, C] extends scala.collection.Map[String, ColumnFamily[_]] with Base[A, B, C] {
  /**
   * ColumnFamily map instantiated using client instance
   */
  case class ColumnFamily(columnFamily : String) extends StandardColumnFamily[A, B, C](client.Path(columnFamily), client) {}

  /**
   * SuperColumnFamily map instantiated using client instance
   */
  case class SuperColumnFamily(columnFamily : String) extends SCF[A, B, C](client.Path(columnFamily), client) {}
  val keyspace : String

  lazy private val schema = { client.describe }
  lazy private val columnFamilies = {
    schema.map { case(name, description) =>
      (name -> buildColumnFamily(name))
    }
  }

  def get(columnFamily : String) : Option[CF[_]] = {
    schema.get(columnFamily) match {
      case None => None
      case Some(cF) => Some(buildColumnFamily(columnFamily))
    }
  }

  def elements = columnFamilies.elements

  def size = schema.size

  private def buildColumnFamily(columnFamily : String) : CF[_] = {
    schema(columnFamily)("Type") match {
      case "Super" => SuperColumnFamily(columnFamily)
      case "Standard" => ColumnFamily(columnFamily)
    }
  }

  override def toString() = {
    "Keyspace(" + columnFamilies.map { case (name, instance) =>
      name + " -> " + instance.getClass.getSimpleName
    }.mkString(",") + ")"
  }
}
