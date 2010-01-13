package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra.serializer.StringSerializer

object ClientTest extends Specification {
  var connection : Connection = Connection()
  val cassandra = new Client(connection, "Keyspace1", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)
  import cassandra.{StandardSlice, SuperSlice}
  
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

  // Clean up when we are dome
  doLast {
    cassandra.remove(key, path)
    cassandra.remove(key, superPath)
    connection.close
  }

  "insert" should {
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

  "count" should {
    "should be able to count columns in a row" in {
      cassandra.count(key, path) must equalTo(jsmith.size)
    }
    "should be able to count supercolumns" in {
      cassandra.count(key, superPath) must equalTo(index.size)
    }
    "should be able to count columns in a supercolumn" in {
      cassandra.count(key, superPath / Some("1")) must equalTo(index("1").size)
    }
  }

  "column slicing" should {
    "return columns using column list as filter" in {
      val result = cassandra.get(key, path, StandardSlice(List("first", "age")))
      result.size must be(2)
      result must not have the key("last")
    }

    "be able to filter columns using start and finish parameter" in {
      val result = cassandra.get(key, path, StandardSlice(Range(Some("first"), Some("last"), Ascending, 1000)))
      result.size must be(3)
      result.keySet must containAll(List("first", "foo", "last"))
    }

    "be able to filter columns using only start parameter" in {
      val result = cassandra.get(key, path, StandardSlice(Range(Some("first"), None, Ascending, 1000)))
      result.size must be(3)
      result.keySet must containAll(List("first", "foo", "last"))
    }

    "be able to filter columns using only finish parameter" in {
      val result = cassandra.get(key, path, StandardSlice(Range(None, Some("last"), Ascending, 2)))
      result.size must be(2)
      result.keySet must containAll(List("age", "first"))
    }

    "use sort parameter to sort columns" in {
      val result = cassandra.get(key, path, StandardSlice(Range[String](None, None, Descending, 1)))
      result must haveKey("last")
    }

    "be able to limit results" in {
      val result = cassandra.get(key, path, StandardSlice(Range[String](None, None, Descending, 2)))
      result.size must be(2)
    }

    "work on super scolumns" in {
      val result = cassandra.get(key, superPath / Some("1"), StandardSlice(Range[String](None, None, Descending, 1)))
      result must haveKey("foo")
    }
  }

  "super column slicing" should {
    "work using collection of super columns" in {
      val result = cassandra.get(key, superPath, SuperSlice(List("2", "3")))
      result.size must be(2)
      result must not have the key("1")
    }

    "contain standard column data" in {
      val result = cassandra.get(key, superPath, SuperSlice(List("1", "3")))
      result.size must be(2)
      result("3")("nothing") must equalTo("here")
    }

    "be able to filter columns using start and finish parameter" in {
      val result = cassandra.get(key, superPath, SuperSlice(Range(Some("1"), Some("2"), Ascending, 1000)))
      result.size must be(2)
      result must haveKey("1")
      result must haveKey("2")
    }
  }

  "single column fetching" should {
    "be able to get super column using path" in {
      val result = cassandra.get(key, superPath / Some("3"))
      result must beSomething
      result.get.keySet must containAll(index("3").keySet)
    }

    "return None when column is not found" in {
      cassandra.get(key, superPath / Some("doesntexist")) must beNone
    }

    "be able to get column from super column using path" in {
      val result = cassandra.get(key, (superPath / Some("2") / "blah"))
      result must equalTo(Some("meh"))
    }

    "be able to get column from standard column family using path" in {
      val result = cassandra.get(key, path / "first")
      result must equalTo(Some(jsmith("first")))
    }

  }
  
  "multiget" should {
    for(i <- (0 until 5)) {
      cassandra(i.toString, cassandra.ColumnParent("Standard1", None)) = jsmith
    }

    "find all existing records" in {
      val result = cassandra.multiget(List("1", "3", "6"), cassandra.ColumnPath("Standard1", None, "last"))

      result("1") must beSomething
      result("3") must beSomething
      result("6") must beNone
      result must not have the key("2")
    }

    "find existing rows" in {
      val result = cassandra.getAll(List("1", "6"), cassandra.ColumnParent("Standard1", None))
      
      result("1") must haveSize(jsmith.size)
      result("6") must beEmpty
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
        cassandra.insertSuper(key, superPath, Map("1" -> Map(key -> data)))
      }

      cassandra.insertSuper(key, superPath / None, Map("1" -> superData))

      cassandra.get(key, superPath / Some("1")).get.keys.toList must containInOrder(superData.map(_._1).toList)
    }

    "work on standard columns" in {
      val path = cassandra.ColumnParent("Standard1", None)
      val key = "ordering-test"
      cassandra.remove(key, path)
      val data = (0 until 25).map { i =>
        (('z' - i).toChar.toString -> i.toString)
      }.toList

      cassandra.insertNormal(key, path, data)
      cassandra.get(key, path, StandardSlice(Range[String](None, None, Descending, 1000))) must containInOrder(data)
    }
  }

  "key ranges" should {
    "be able to list all key ranges in a keyspace" in {
      (cassandra.keys("Super1", None, None)) must containAll(List("jsmith-test", "ordering-test"))
    }
  }


  "remove" should {
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
