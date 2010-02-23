package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra.serializer.StringSerializer

object ClientTest extends Specification {
  shareVariables()
  val connection = Connection(9162)
  val cassandra = new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)
  import cassandra.{StandardSlice, SuperSlice, ColumnParent, ColumnPath}
  
  doLast {
    connection.close
  }
  
  "data modification" should {
    setSequential()
    /* Data to be inserted */
    val jsmith = Map("first" -> "John", "last" -> "Smith", "age" -> "53", "foo" -> "bar")

    // Location for data in standard CF
    val path = cassandra.ColumnParent("Standard1", None)
    // Location for data in super CF
    val superPath = cassandra.ColumnParent("Super1", None)

    // Row key
    val key = "test/jsmith"

    // Index data, for SCF
    val index = Map("1" -> Map("foo" -> null, "bar" -> null), "2" -> Map("blah" -> "meh"), "3" -> Map("nothing" -> "here"))
    
    "insert" in {
      "be able to add and get data to a normal column family" in {
        // Given: John is inserted to Cassandra
        jsmith("first") must equalTo("John")
        cassandra(key, path) = jsmith

        // Then: It should still have its old values.
        val result = cassandra.get(key, path, StandardSlice(Range[String](None, None, Ascending, 1000)))
        result("first") must equalTo(jsmith("first"))
        result("last") must equalTo(jsmith("last"))
        result("age") must equalTo(jsmith("age"))
      }

      "be able to add and get data to a super column family" in {
        // Given: Data is inserted to Cassandra
        cassandra(key, superPath) = index

        // Then: It should still have its old values.
        val result = cassandra.get(key, superPath, SuperSlice(Range[String](None, None, Ascending, 1000)))
        result.keySet must containAll(List("1", "2", "3"))
        result("2")("blah") must equalTo("meh")
      }

      "should be able to insert single value" in {
        val value = (Math.random * 10000).toInt.toString
        val path = cassandra.ColumnPath("Standard1", None, "Random")
        val key = "random-test"
        // Given: Value is inserted to Cassandra
        cassandra(key, path) = value

        // Then: It should be readable from Cassandra.
        cassandra.get(key, path) must beSomething.which(_ must equalTo(value))
      }

    }
    
    "remove" in {
      "be able to remove single column" in {
        // Given that age column is defined and the column is removed
        cassandra.get(key, path, StandardSlice(Range[String](None, None, Ascending, 1000))) must haveKey("age")
        cassandra.remove(key, path / "age")

        // Then: age column should not have value
        cassandra.get(key, path / "age") must beNone
      }

      "be able to remove entire row" in {
        cassandra.get(key, cassandra.ColumnPath("Standard1", None, "first")) must beSomething
        // Given: John is removed from Cassandra
        cassandra.remove(key, cassandra.ColumnParent("Standard1", None))
        // Then: It should not return anything
        cassandra.get(key, cassandra.ColumnPath("Standard1", None, "first")) must beNone
      }
    }
  }

  "count" should {
    val path = ColumnParent("Standard1", None)
    val superPath = ColumnParent("Super1", None)
    val data = Map("lol" -> "cat", "cheez" -> "burger")
    doFirst {
      cassandra("count", path) = data
      cassandra("count", superPath) = Map("internet" -> data)
    }

    "should be able to count columns in a row" in {
      cassandra.count("count", path) must eventually(equalTo(data.size))
    }
    "should be able to count supercolumns" in {
      cassandra.count("count", superPath) must eventually(equalTo(1))
    }
    "should be able to count columns in a supercolumn" in {
      cassandra.count("count", superPath / Some("internet")) must eventually(equalTo(data.size))
    }

    doLast {
      cassandra.remove("count", path)
      cassandra.remove("count", superPath)
    }
  }

  "column slicing" should {
    // Paths for data
    val path = cassandra.ColumnParent("Standard1", None)
    // Row key
    val key = "slicing"
    
    doFirst { // Insert data
      val jsmith = Map("first" -> "John", "last" -> "Smith", "age" -> "53")      
      cassandra(key, path) = jsmith
    }

    "return columns using column list as filter" in {
      val result = cassandra.get(key, path, StandardSlice(List("first", "age")))
      result must eventually(haveSize(2))
      result must not have the key("last")
    }

    "be able to filter columns using start and finish parameter" in {
      val result = cassandra.get(key, path, StandardSlice(Range(Some("first"), Some("last"), Ascending, 1000)))
      result must eventually(haveSize(2))
      result.keySet must containAll(List("first", "last"))
    }

    "be able to filter columns using only start parameter" in {
      val result = cassandra.get(key, path, StandardSlice(Range(Some("first"), None, Ascending, 1000)))
      result must eventually(haveSize(2))
      result.keySet must containAll(List("first", "last"))
    }

    "be able to filter columns using only finish parameter" in {
      val result = cassandra.get(key, path, StandardSlice(Range(None, Some("last"), Ascending, 2)))
      result.size must be(2)
      result.keySet must containAll(List("age", "first"))
    }

    "use sort parameter to sort columns" in {
      val result = cassandra.get(key, path, StandardSlice(Range[String](None, None, Descending, 1)))
      result must eventually(haveKey("last"))
    }

    "be able to limit results" in {
      val result = cassandra.get(key, path, StandardSlice(Range[String](None, None, Descending, 2)))
      result must eventually(haveSize(2))
    }

    doLast {
      cassandra.remove(key, path)
    }
  }

  "super column slicing" should {
    val superPath = cassandra.ColumnParent("Super1", None)
    val key = "slicing"
    doFirst {
      val index = Map("1" -> Map("foo" -> null, "bar" -> null), "2" -> Map("blah" -> "meh"), "3" -> Map("nothing" -> "here"))
      cassandra(key, superPath) = index
    }

    "work using collection of super columns" in {
      val result = cassandra.get(key, superPath, SuperSlice(List("2", "3")))
      result must eventually(haveSize(2))
      result must not have the key("1")
    }

    "contain standard column data" in {
      val result = cassandra.get(key, superPath, SuperSlice(List("1", "3")))
      result must eventually(haveSize(2))
      result("3")("nothing") must eventually(equalTo("here"))
    }

    "be able to filter columns using start and finish parameter" in {
      val result = cassandra.get(key, superPath, SuperSlice(Range(Some("1"), Some("2"), Ascending, 1000)))
      result must eventually(haveSize(2))
      result must eventually(haveKey("1"))
      result must eventually(haveKey("2"))
    }
    
    "work on subcolumns" in {
      val result = cassandra.get(key, superPath / Some("1"), StandardSlice(Range[String](None, None, Descending, 1)))
      result must eventually(haveKey("foo"))
    }

    doLast {
      cassandra.remove(key, superPath)
    }
  }

  "single column fetching" should {
    val path = ColumnParent("Super1", None)
    val key = "singleColumnFetch"
    val data = Map("1" -> Map("2" -> "3", "4" -> "5"))
    doFirst {
      cassandra(key, path) = data
      cassandra(key, ColumnParent("Standard1", None)) = data("1")
    }

    "be able to get super column using path" in {
      cassandra.get(key, path / Some("1")) must eventually(beSomething)
      cassandra.get(key, path / Some("1")).get.keySet must eventually(containAll(data("1").keySet))
    }

    "return None when column is not found" in {
      cassandra.get(key, path / Some("doesntexist")) must eventually(beNone)
    }

    "be able to get column from super column using path" in {
      cassandra.get(key, (path / Some("1") / "2")) must eventually(equalTo(Some("3")))
    }

    "be able to get column from standard column family using path" in {
      val result = cassandra.get(key,  ColumnPath("Standard1", None, "4"))
      result must equalTo(Some(data("1")("4")))
    }

    doLast {
      cassandra.remove(key, path)
      cassandra.remove(key, ColumnParent("Standard1", None))
    }
  }
  
  "multiple record fetching" should {
    doFirst {
      for(i <- (0 until 5)) {
        cassandra("multiget:" + i.toString, ColumnParent("Standard1", None)) = Map("test" -> "data", "foo" -> "bar")
      }
    }

    "find all existing records" in {
      val result = cassandra.get(List("multiget:1", "multiget:3", "multiget:6"), ColumnPath("Standard1", None, "foo"))

      result("multiget:1") must beSomething
      result("multiget:3") must beSomething
      result("multiget:6") must beNone
      result must not have the key("2")
    }

    doLast {
      for(i <- (0 until 5)) {
        cassandra.remove("multiget:" + i.toString, ColumnParent("Standard1", None))
      }
    }
  }

  "multiple record slicing" should {
    doFirst {
      val data = Map("a" -> "b", "c" -> "d")
      val sdata = Map("a" -> Map("b" ->"c"), "d" -> Map("e" -> "f"))
      
      for(i <- (0 until 5)) {
        cassandra("multi:" + i.toString, ColumnParent("Standard1", None)) = data
        cassandra("multi:" + i.toString, ColumnParent("Super1", None)) = sdata
      }
    }
    
    "using key range" in {
      "find values from standard column family" in {
        val r = cassandra.get(ColumnParent("Standard1", None), StandardSlice(List("a", "c")), Some("multi:2"), None, 3)
        r.size must be(3)
        r must haveKey("multi:2")
        r("multi:2") must haveKey("a")
      }
      "find values from super column family" in {
        val r = cassandra.get(ColumnParent("Super1", None), SuperSlice(List("a", "d")), Some("multi:3"), Some("multi:4"), 3)
        r.size must be(2)
        r must haveKey("multi:3")
        r must haveKey("multi:4")
        r("multi:4") must haveKey("a")
      }
    }
    
    "using list of keys" in {
      val keys = List("multi:1", "multi:3")
      "find values from standard column family" in {
        val r = cassandra.get(keys, ColumnParent("Standard1", None), StandardSlice(List("a", "e")))
        r must haveKey("multi:1")
        r("multi:1") must haveKey("a")
        r("multi:3") must notHaveKey("b")
      }
      "find values from super column family" in {
        val r = cassandra.get(keys, ColumnParent("Super1", None), SuperSlice(Range(Some("a"), Some("d"), Ascending, 100)))
        r("multi:1") must haveKey("a")
        r("multi:1") must notHaveKey("b")
        r("multi:3") must haveKey("d")
        r("multi:3")("d") must haveKey("e")
      }
      
      doLast {
        for(i <- (0 until 5)) {
          cassandra.remove("multi:" + i.toString, ColumnParent("Standard1", None))
          cassandra.remove("multi:" + i.toString, ColumnParent("Super1", None))
        }
      }
    }
  }

  "order preservation" should {

    // Ugly hack
    def pad(s : Int, length : Int) : String = {
      ("0" * (length - s.toString.length)) + s.toString
    }

    "work on columns in super column family" in {
      val superPath = cassandra.ColumnParent("Super1", None)
      val key = "ordering-test"
      val randomizer = new scala.util.Random(10)
      cassandra.remove(key, superPath)
      val superData = (0 until 50).map(pad(_, 3) ->(randomizer.nextInt.toString)).toList

      superData.map { case(key, data) =>
        cassandra(key, superPath) = Map("1" -> Map(key -> data))
      }

      cassandra(key, superPath / None) =  Map("1" -> superData)

      cassandra.get(key, superPath / Some("1")).get.keys.toList must containInOrder(superData.map(_._1).toList)
    }

    "work on standard columns" in {
      val path = cassandra.ColumnParent("Standard1", None)
      val key = "ordering-test"
      cassandra.remove(key, path)
      val data = (0 until 25).map { i =>
        (('z' - i).toChar.toString -> i.toString)
      }.toList

      cassandra(key, path) = data
      cassandra.get(key, path, StandardSlice(Range[String](None, None, Descending, 1000))) must eventually(containInOrder(data))
    }
  }
  
  "key ranges" should {
    doFirst {
      // Insert data
      val p1 = ColumnPath("Super1", Some("superColumn"), "column")
      cassandra("range1", p1) = "foo"
      cassandra("range2", p1) = "bar"
      
      val p2 = ColumnPath("Standard1", None, "column")
      cassandra("range1", p2) = "foo"
      cassandra("range2", p2) = "foo"
    }

    "be able to list key ranges" in {
      val path = ColumnParent("Super1", None)
      val r = cassandra.get(path, SuperSlice(List("superColumn")), Some("range"), Some("range3"), 100)
      r must haveKey("range1")
      r must haveKey("range2")
    }
    
    "contain super column data" in {
      val path = ColumnParent("Super1", None)
      val r = cassandra.get(path, SuperSlice(List("superColumn")), Some("range"), Some("range3"), 100)
      r must haveKey("range1")
      r("range1") must haveKey("superColumn")
    }
    
    "contain column data in super column" in {
      val path = ColumnParent("Super1", Some("superColumn"))
      val r = cassandra.get(path, StandardSlice(List("column")), Some("range"), Some("range3"), 100)
      
      r must haveKey("range1")
      r("range1") must haveKey("column")
    }
    
    "contain column data from standard column family" in {
      val path = ColumnParent("Standard1", None)
      val r = cassandra.get(path, StandardSlice(List("column")), Some("range"), Some("range3"), 100)
      
      r must haveKey("range1")
      r("range1") must haveKey("column")
    }
    
    "not contain any data if empty slice is given" in {
      val path = ColumnParent("Standard1", None)
      val r = cassandra.get(path, StandardSlice(Nil), Some("range"), Some("range3"), 100)
      
      r must haveKey("range1")
      r("range1") must notHaveKey("column")
    }

    doLast {
      // Remove data
      cassandra.remove("range1", ColumnParent("Super1", None))
      cassandra.remove("range2", ColumnParent("Super1", None))
      
      cassandra.remove("range1", ColumnParent("Standard1", None))
      cassandra.remove("range2", ColumnParent("Standard1", None))
    }
  }
}
