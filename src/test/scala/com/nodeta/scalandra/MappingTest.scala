package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra.map._
import com.nodeta.scalandra.serializer._

object MappingTest extends Specification {
/*  "Keyspace" should {
    val connection = Connection()
    doLast { connection.close() }

    val keyspace = new Keyspace[String, String, String] {
      protected val client = Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)
      val keyspace = "Keyspace1"
    }
    "be able to list its ColumnFamilies" in {
      keyspace.keySet must containAll(List("Standard1", "Standard2"))
    }

  }
*/
  "ColumnFamily" should {
    val connection = Connection()
    val cassandra = new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)
    import cassandra.{ColumnPath, ColumnParent}

    doFirst {
      val path = ColumnPath("Standard1", None, "test")
      for(i <- (0 until 200)) {
        cassandra("keys:" + i, path) = i.toString
      }
    }
    
    doLast {
      val path = ColumnPath("Standard1", None, "test")
      for(i <- (0 until 200)) {
        cassandra.remove("keys:" + i, path)
      }
    }
    
    "list its rows" in {
      cassandra.ColumnFamily("Standard1").size must beGreaterThanOrEqualTo(200)
    }
  }

  "StandardColumnFamily" should {
    val connection = Connection()
    val client = new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)

    val cf = new StandardColumnFamily("Standard1", client)

    "be able to list all rows" in {
      val key = Math.random.toString.substring(2)
      client(key, client.ColumnPath("Standard1", None, "test")) = "value"
      cf.keySet must contain(key)
    }
    
    "multiget values" in {
      cf.map("test") must notBeEmpty
    }
    
    "row modification" in {
      val key = Math.random.toString.substring(2)
      
      "insert new data" in {
        cf(key) = Map("test" -> "value")
        client.get(key, client.ColumnPath("Standard1", None, "test")) must beSomething
        client.remove(key, client.Path("Standard1"))
      }
      
      "remove data" in {
        client(key, client.ColumnPath("Standard1", None, "test2")) = "lolz"
        cf.remove(key)
        client.get(key, client.ColumnPath("Standard1", None, "test2")) must beNone
      }
    }

    "be able to create row instances without any requests" in {
      connection.close() // Connection should not be needed
      try {
        val cf = new StandardColumnFamily("Standard1", client)

        val r = cf("Row")
        cf.get("RowFooasoafso")
      } catch {
        case _ => fail("Thrift was called")
      }
      connection.isOpen must be(false)
    }
    
    
    "be able to slice using lists" in {
      cf.slice(List("foo", "bar")).keySet must haveSize(2)
    }
    
    
    "be able to slice using range" in {
      cf.slice(Range[String](None, None, Ascending, 100)).keySet must haveSize(100)
    }
  }
  
  "SuperdColumnFamily" should {
    val connection = Connection()
    val client = new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)

    val cf = new SuperColumnFamily("Super1", client)

    "be able to list all rows" in {
      val key = Math.random.toString.substring(2)
      client(key, client.ColumnPath("Super1", Some("test"), "test")) = "value"
      cf.keySet must contain(key)
    }
    
    "row modification" in {
      val key = Math.random.toString.substring(2)
      
      "insert new data" in {
        cf(key) = Map("lol" -> Map("test" -> "value"))
        client.get(key, client.ColumnPath("Super1", Some("lol"), "test")) must beSomething
        client.remove(key, client.Path("Super1"))
      }
      
      "remove data" in {
        client(key, client.ColumnPath("Super1", Some("cat"), "test2")) = "lolz"
        cf.remove(key)
        client.get(key, client.ColumnPath("Super1", Some("cat"), "test2")) must beNone
      }
    }
  }
  /*

  "StandardRecord" should {
    val connection = Connection()
    val _client = new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)

    def createRecord() : StandardRecord[String, String, String] = {
      new StandardRecord[String, String, String] {
        protected val columnFamily = "Standard1"
        protected val client = _client
        protected val key : String = "row-test"
      }
    }

    val rowData = Map((0 until 20).map { i =>
      val s = ('a' + i).toChar.toString
      (s -> s)
    } : _*)

    _client.insertNormal("row-test", _client.ColumnParent("Standard1", None), rowData)
    val row = createRecord()

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

    "be able to insert values to a record" in {
      row("a") = "b"
      row.slice(List("a"))("a") must equalTo("b")
    }

    "be able to remove values from a record" in {
      row.slice(List("a")) must haveSize(1)
      row -= "a"
      row.slice(List("a")) must haveSize(0)
    }

    "not request anything when created" in {
      connection.close()
      try {
        createRecord()
      } catch {
        case _ => fail("Request is made")
      }
      connection.isOpen must equalTo(false)
    }
   }

  "SuperRecord" should {
    val connection = Connection()
    val _client = new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)

    def createRecord() : SuperRecord[String, String, String] = {
      new SuperRecord[String, String, String] {
        protected val columnFamily = "Super1"
        protected val client = _client

        protected val key = "superrow-test"
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

    _client.insertSuper("superrow-test", _client.ColumnParent("Super1", None), rowData)

    val row = createRecord()

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

    "be able to remove values from a record" in {
      row.slice(List("a")) must haveSize(1)
      row -= "a"
      row.slice(List("a")) must haveSize(0)
    }
  }

  "SuperColumn" should {
    val connection = Connection()
    val _client = new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)
    val columnPath = _client.ColumnParent("Super1", Some("b"))


    def createSuperColumn() : SuperColumn[String, String, String] = {
      new SuperColumn[String, String, String] {
        protected val columnFamily = "Standard1"
        protected val client = _client
        protected val path = columnPath
        protected val key = "superrow-test"
      }
    }

    def buildMap(n : Int) : Map[String, String] = {
      Map((0 until n).map { i =>
        val s = ('a' + i).toChar.toString
        (s -> s)
      } : _*)
    }

    _client.insertSuper("superrow-test", columnPath, Map("b" -> buildMap(20)))

    val row = createSuperColumn()

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

    "be able to remove values from a super column" in {
      row.slice(List("a")) must haveSize(1)
      row -= "a"
      row.slice(List("a")) must haveSize(0)
    }
  }
*/
}
