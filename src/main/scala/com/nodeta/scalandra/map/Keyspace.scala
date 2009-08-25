package com.nodeta.scalandra.map

import scala.collection.jcl.Conversions

trait Keyspace[A, B, C] extends scala.collection.Map[String, ColumnFamily[_]] with SuperBase[A, B, C] {
  val keyspace : String
  protected val connection : Connection

  lazy private val schema = { client.describe }
  lazy private val columnFamilies = {
    schema.map { case(name, description) =>
      (name -> buildColumnFamily(name))
    }
  }

  def get(columnFamily : String) : Option[ColumnFamily[_]] = {
    schema.get(columnFamily) match {
      case None => None
      case Some(cF) => Some(buildColumnFamily(columnFamily))
    }
  }

  def elements() = {
    columnFamilies.elements
  }


  def size() = {
    schema.size
  }

  private def buildColumnFamily(_columnFamily : String) : ColumnFamily[_] = {
    val parent = this
    def buildSuper() : SuperColumnFamily[A, B, C] = {
      new SuperColumnFamily[A, B, C] {
        protected val columnSerializer = parent.columnSerializer
        protected val superColumnSerializer = parent.superColumnSerializer
        protected val valueSerializer = parent.valueSerializer

        protected val keyspace = parent.keyspace
        protected val columnFamily = _columnFamily
        protected val connection = parent.connection
      }
    }

    def buildStandard() : StandardColumnFamily[B, C] = {
      new StandardColumnFamily[B, C] {
        protected val columnSerializer = parent.columnSerializer
        protected val valueSerializer = parent.valueSerializer

        protected val keyspace = parent.keyspace
        protected val columnFamily = _columnFamily
        protected val connection = parent.connection
      }
    }

    schema(_columnFamily)("Type") match {
      case "Super" => buildSuper()
      case "Standard" => buildStandard()
    }
  }
}