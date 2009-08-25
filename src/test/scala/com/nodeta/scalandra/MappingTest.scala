package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra.map._
import com.nodeta.scalandra.serializer._

object MappingTest extends Specification {  
  "Keyspace" should {
    val _connection = Connection()
    doLast { _connection.close() }
    
    val keyspace = new Keyspace[String, String, String] {
      protected val connection = Connection()
      val keyspace = "Keyspace1"
      
      protected val columnSerializer = StringSerializer
      protected val superColumnSerializer = StringSerializer
      protected val valueSerializer = StringSerializer
    }
    "be able to list its ColumnFamilies" in {
      keyspace.keySet must containAll(List("Standard1", "Standard2"))
    }
    
  }
  
  
  "StandardColumnFamily" should {
    val _connection = Connection()
    val client = new Client(_connection, "Keyspace1", StringSerializer, StringSerializer, StringSerializer)
    
    val cf = new StandardColumnFamily[String, String] {
      val keyspace = "Keyspace1"
      val columnFamily = "Standard1"
      val connection = _connection
      
      val columnSerializer = StringSerializer
      val valueSerializer = StringSerializer
    }
      
    "be able to list all rows" in {
      client.insertNormal(ColumnParent[String]("Standard1", "test-row"), Map("foo" -> "bar"))
      cf.keySet must contain("test-row")
    }

    "be able to create rows without any requests" in {
      _connection.close() // Connection should not be needed
      try {
        val cf = new StandardColumnFamily[String, String] {
          val keyspace = "Keyspace1"
          val columnFamily = "Standard1"
          val connection = _connection

          val columnSerializer = StringSerializer
          val valueSerializer = StringSerializer
        }
        
        val r = cf("Row")
        cf.get("RowFooasoafso")
      } catch {
        case _ => fail("Thrift was called")
      }
      _connection.isOpen must be(false)
    }
  }
  
  "StandardRow" should {
    val _connection = Connection()
    val client = new Client(_connection, "Keyspace1", StringSerializer, StringSerializer, StringSerializer)

    def createRow() : StandardRow[String, String] = {
      new StandardRow[String, String] {
        protected val keyspace = "Keyspace1"
        protected val columnFamily = "Standard1"
        protected val connection = _connection

        protected val columnSerializer = StringSerializer
        protected val valueSerializer = StringSerializer

        protected val path = ColumnParent[Any]("Standard1", "row-test")
      }
    }
    
    val rowData = Map((0 until 20).map { i =>
      val s = ('a' + i).toChar.toString
      (s -> s)
    } : _*)
    
    client.insertNormal(ColumnParent[String]("Standard1", "row-test"), rowData)
    val row = createRow()
    
    "provide slicing functionality by names" in {
      val q = List("a", "b", "f")
      val r = row.slice(q)
      r.keySet must containAll(q)
    }
    
    "be able to slice columns by range" in {
      val r = row.slice("e", "k")
      r.keySet must containAll(List("e", "j", "k"))
    }
    
    "be able to list its columns" in {
      row.keySet.size must be(20)
    }
    
    "not request anything when created" in {
      _connection.close()
      try {
        createRow()
      } catch {
        case _ => fail("Request is made")
      }
      _connection.isOpen must equalTo(false)
    }
   }
   
  "SuperRow" should {
    val _connection = Connection()
    val client = new Client(_connection, "Keyspace1", StringSerializer, StringSerializer, StringSerializer)
    
    def createRow() : SuperRow[String, String, String] = {
      new SuperRow[String, String, String] {
        protected val keyspace = "Keyspace1"
        protected val columnFamily = "Standard1"
        protected val connection = _connection

        protected val columnSerializer = StringSerializer
        protected val superColumnSerializer = StringSerializer
        protected val valueSerializer = StringSerializer

        protected val path = ColumnParent[String]("Super1", "superrow-test")
      }
    }
    
    def buildMap(n : Int) : Map[String, String] = {
      Map((0 until n).map { i =>
        val s = ('a' + i).toChar.toString
        (s -> s)
      } : _*)
    }
    
    val rowData = List((0 until 20).map { i =>
      val s = ('a' + i).toChar.toString
      (s -> buildMap(i+1))
    } : _*)
    
    client.insertSuper(ColumnParent[String]("Super1", "superrow-test"), rowData)
    
    val row = createRow()
    
    "provide slicing functionality by names" in {
      val q = List("f", "g", "l")
      val r = row.slice(q)
      r.keySet must containAll(q)
    }
    
    "be able to slice columns by range" in {
      val r = row.slice("a", "f")
      r.keySet must containAll(List("a", "c", "f"))
    }
    
    "be able to list its columns" in {
      row.keySet.size must be(20)
    }
  }
}
