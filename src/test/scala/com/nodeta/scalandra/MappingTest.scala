package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra.map._
import com.nodeta.scalandra.serializer._

object MappingTest extends Specification {
  def client(c : Connection) : Client[String, String, String] = {
    new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)
  }
  
  def cassandraMap[A, B](map : CassandraMap[String, A], data : scala.collection.Map[String, B]) = {
    val keys = map.keySet.toList.sort(_.compareTo(_) < 0)
    "be able to slice by names" in {
      val l = List(keys.first, keys.last)
      map.slice(l).keySet must containAll(l)
    }
    "be able to slice by range" in {
      val l = keys.drop(1).dropRight(1)

      val r = map.slice(Range(Some(l.first), Some(l.last), Ascending, l.size))
      r.keySet must haveTheSameElementsAs(l)
    }
  }
  
  def record[A, B](map : CassandraMap[String, A], data : scala.collection.Map[String, B], lazyFetch : Boolean) = {
    cassandraMap(map, data)
    "be able to remove column" in {
      val key = map.keySet.toList.last
      map.slice(List(key)) must haveSize(1) // .get may be lazy, so slice is used instead
      map.remove(key)
      Thread.sleep(25)
      map.slice(List(key)) must haveSize(0)
    }
    
    "be able to list its columns" in {
      map.keySet must haveSize(data.size)
    }
    
    
    "column access" in {
      "existing column" in {
        "get returns column in option" in {
          map.get(data.keys.next) must beSomething
        }

        "apply returns column value" in {
          val key = data.keys.next
          map(key) must equalTo(data(key))
        }
      }
      
      if (!lazyFetch) {
        "nonexisting column" in {
          "get returns none" in {
            map.get("zomg, doesn't exist") must beNone
          }

          "apply raises exception" in {
            map("zomg, doesn't exist") must throwA[java.util.NoSuchElementException]
          }
        }
      }
    }
  }


  val connection = Connection(9162)
  val cassandra = client(connection)

  doAfter {
    connection.close()
  }
  
  doLast {
    val connection = Connection(9162)
    val cassandra = client(connection)
    0.until(20).foreach { i =>
      cassandra.remove("row-" + i, cassandra.ColumnParent("Standard1", None))
      cassandra.remove("row-" + i, cassandra.ColumnParent("Super1", None))
    }
  }

  "Keyspace" should {
    val keyspace = new Keyspace[String, String, String] {
      protected val client = cassandra
      val keyspace = "Keyspace1"
    }

    "be able to list its ColumnFamilies" in {
      keyspace.keySet must containAll(List("Standard1", "Standard2"))
    }
  }

  "ColumnFamily" should {
    val data = insertStandardData()
    
    "list its rows" in {
      cassandra.ColumnFamily("Standard1").keySet must containAll(data.keySet)
    }
  }

  "StandardColumnFamily" should {
    val data = insertStandardData()
    val cf = new StandardColumnFamily("Standard1", cassandra)

    "provide cassandra map functionality" in cassandraMap(cf, data)
    
    "multiget values" in {
      val key = data.values.next.keys.next
      cf.map(key) must notBeEmpty
    }
    
    "row modification" in {
      val key = Math.random.toString.substring(2)
      
      "insert new data" in {
        cf(key) = Map("test" -> "value")
        cassandra.get(key, cassandra.ColumnPath("Standard1", None, "test")) must beSomething
        cassandra.remove(key, cassandra.Path("Standard1"))
      }
      
      "remove data" in {
        cassandra(key, cassandra.ColumnPath("Standard1", None, "test2")) = "lolz"
        cf.remove(key)

        Thread.sleep(25)
        cassandra.get(key, cassandra.ColumnPath("Standard1", None, "test2")) must beNone
      }
    }

    "be able to create row instances without any requests" in {
      connection.close() // Connection should not be needed
      try {
        val cf = new StandardColumnFamily("Standard1", cassandra)

        val r = cf("Row")
        cf.get("RowFooasoafso")
      } catch {
        case _ => fail("Thrift was called")
      }
      connection.isOpen must be(false)
    }
  }
  
  "SuperColumnFamily" should {
    val data = insertSuperData()
    val cf = new SuperColumnFamily("Super1", cassandra)
    
    "provide cassandra map functionality" in cassandraMap(cf, data)
    
    "row modification" in {
      val key = Math.random.toString.substring(2)
      
      "insert new data" in {
        cf(key) = Map("lol" -> Map("test" -> "value"))
        Thread.sleep(25)
        cassandra.get(key, cassandra.ColumnPath("Super1", Some("lol"), "test")) must beSomething
        cassandra.remove(key, cassandra.Path("Super1"))
      }
      
      "remove data" in {
        cassandra(key, cassandra.ColumnPath("Super1", Some("cat"), "test2")) = "lolz"
        cf.remove(key)
        
        Thread.sleep(25)
        cassandra.get(key, cassandra.ColumnPath("Super1", Some("cat"), "test2")) must beNone
      }
    }
  }

  "StandardRecord" should {
    val Pair(key, data) = insertStandardData().elements.next
    val row = new StandardRecord(key, cassandra.ColumnParent("Standard1", None), cassandra)
    
    "provide cassandra map functionality" in record(row, data, false)

    "load data lazily from cassandra" in {
      connection.close()
      "while creating new instance" in {
        try {
          new StandardRecord("row-test", cassandra.ColumnParent("Standard1", None), cassandra)
        } catch {
          case _ => fail("Request is made")
        }
        connection.isOpen must equalTo(false)
      }
    }
  }

  "SuperColumn" should {
    val Pair(key, row) = insertSuperData().elements.next
    val Pair(superColumn, data) = row.elements.next

    val r = new StandardRecord(key, cassandra.ColumnParent("Super1", Some(superColumn)), cassandra)
    
    "provide cassandra map functionality" in record(r, data, false)

    "load data lazily from cassandra" in {
      connection.close()
      "while creating new instance" in {
        try {
          new StandardRecord("row-test", cassandra.ColumnParent("Super1", Some(superColumn)), cassandra)
        } catch {
          case _ => fail("Request is made")
        }
        connection.isOpen must equalTo(false)
      }
    }
  }


  "SuperRecord" should {
    val Pair(key, data) = insertSuperData().elements.next
    val row = new SuperRecord(key, cassandra.Path("Super1"), cassandra)

    "provide cassandra map functionality" in record(row, data, true)

    "load data lazily from cassandra" in {
      connection.close()
      "while creating new instance" in {
        try {
          new SuperRecord("row-test", cassandra.ColumnParent("Super1", None), cassandra)
        } catch {
          case _ => fail("Request is made")
        }
        connection.isOpen must equalTo(false)
      }
      "while accessing sub column" in {
        try {
          new SuperRecord("row-test", cassandra.ColumnParent("Super1", None), cassandra).get("LOL")
        } catch {
          case _ => fail("Request is made")
        }
        connection.isOpen must equalTo(false)
      }
    }
  }
  
  def insertStandardData() : Map[String, scala.collection.Map[String, String]] = {
    val connection = Connection(9162)
    val cassandra = client(connection)
    
    val row = Map((0 until 50).map { i =>
      val s = ('a' + i).toChar.toString
      (("column-" + s) -> s)
    } : _*)
    
    try {
      Map(0.until(20).map {
        i => cassandra("row-" + i, cassandra.ColumnParent("Standard1", None)) = row
        ("row-" + i -> row)
      } : _*)
    } finally {
      Thread.sleep(50)
      connection.close()
    }
  }
  
  def insertSuperData() : Map[String, scala.collection.Map[String, scala.collection.Map[String, String]]] = {
    val connection = Connection(9162)
    val cassandra = client(connection)
    
    def buildMap(n : Int) : Map[String, String] = {
      Map((0 until n).map { i =>
        val s = ('a' + i).toChar.toString
        (("column-" + s) -> s)
      } : _*)
    }

    val row = Map((0 until 50).map { i =>
      val s = ('a' + i).toChar.toString
      (("supercolumn-" + s) -> buildMap(i+1))
    } : _*)
    
    try {
      Map(0.until(20).map { i =>
        cassandra("row-" + i, cassandra.Path("Super1")) = row
        (("row-" + i) -> row)
      } : _*)
    } finally {
      Thread.sleep(25)
      connection.close()
    }
  }
}
