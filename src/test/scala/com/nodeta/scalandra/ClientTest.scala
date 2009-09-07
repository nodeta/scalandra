package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra.serializer.StringSerializer

object ClientTest extends Specification {
  val cassandra = new Client(Connection(), "Keyspace1", StringSerializer, StringSerializer, StringSerializer)
  val jsmith = Map("first" -> "John", "last" -> "Smith", "age" -> "53", "foo" -> "bar")
  val path = ColumnParent[String]("Standard1", "jsmith-test")
  val superPath = ColumnParent[String]("Super1", "jsmith-test")
  val index = Map("1" -> Map("foo" -> null, "bar" -> null), "2" -> Map("blah" -> "meh"), "3" -> Map("nothing" -> "here"))

  "insert" should {
    "be able to add and get data to a normal column family" in {
      // Given: John is inserted to Cassandra
      jsmith("first") must equalTo("John")
      cassandra(path) = jsmith

      // Then: It should still have its old values.
      val result = cassandra.slice(path, None, None, Ascending)
      result("first") must equalTo("John")
      result("last") must equalTo("Smith")
      result("age") must equalTo("53")
    }

    "be able to add and get data to a super column family" in {
      // Given: Data is inserted to Cassandra
      cassandra(superPath-) = index

      // Then: It should still have its old values.
      val result = cassandra.sliceSuper(superPath, None, None, Ascending)
      result.keySet must containAll(List("1", "2", "3"))
      result("2")("blah") must equalTo("meh")
    }
    
    "should be able to insert single value" in {
      val value = (Math.random * 10000).toInt.toString
      val path = ColumnPath[String, String]("Standard1", "random-test", None, "Random")
      // Given: Value is inserted to Cassandra
      cassandra(path) = value
      
      // Then: It should be readable from Cassandra.
      cassandra.get(path) must beSomething.which(_ must equalTo(value))
    }
    
  }

  "count" should {
    "should be able to count columns in a row" in {
      cassandra.count(path) must equalTo(jsmith.size)
    }
    "should be able to count supercolumns" in {
      cassandra.count(superPath) must equalTo(index.size)
    }
    "should be able to count columns in a supercolumn" in {
      cassandra.count(superPath ++ "1") must equalTo(index("1").size)
    }
  }

  "column slicing" should {
    "return columns using column list as filter" in {
      val result = cassandra.slice(path, List("first", "age"))
      result.size must be(2)
      result must not have the key("last")
    }

    "be able to filter columns using start and finish parameter" in {
      val result = cassandra.slice(path, Some("first"), Some("last"), Ascending)
      result.size must be(3)
      result.keySet must containAll(List("first", "foo", "last"))
    }

    "be able to filter columns using only start parameter" in {
      val result = cassandra.slice(path, Some("first"), None, Ascending)
      result.size must be(3)
      result.keySet must containAll(List("first", "foo", "last"))
    }

    "be able to filter columns using only finish parameter" in {
      val result = cassandra.slice(path, None, Some("first"), Ascending)
      result.size must be(2)
      result.keySet must containAll(List("age", "first"))
    }

    "use sort parameter to sort columns" in {
      val result = cassandra.slice(path, None, None, Descending, 1)
      result must haveKey("last")
    }

    "be able to limit results" in {
      val result = cassandra.slice(path, None, None, Descending, 2)
      result.size must be(2)
    }

    "work on super scolumns" in {
      val result = cassandra.slice(superPath ++ "1", None, None, Descending, 1)
      result must haveKey("foo")
    }
  }

  "super column slicing" should {
    "work using collection of super columns" in {
      val result = cassandra.sliceSuper(superPath, List("2", "3"))
      result.size must be(2)
      result must not have the key("1")
    }

    "contain standard column data" in {
      val result = cassandra.sliceSuper(superPath, List("1", "3"))
      result.size must be(2)
      result("3")("nothing") must equalTo("here")
    }

    "be able to filter columns using start and finish parameter" in {
      val result = cassandra.sliceSuper(superPath, Some("1"), Some("2"), Ascending)
      result.size must be(2)
      result must haveKey("1")
      result must haveKey("2")
    }
  }

  "single column fetching" should {
    "be able to get super column using path" in {
      Thread.sleep(50)
      val result = cassandra.get(superPath ++ "3")
      result must beSomething
      result.get.keySet must containAll(index("3").keySet)
    }

    "return None when column is not found" in {
      cassandra.get(superPath ++ "doesntexist") must beNone
    }

    "be able to get column from super column using path" in {
      val result = cassandra.get((superPath ++ "2") + "blah")
      result must equalTo(Some("meh"))
    }

    "be able to get column from standard column family using path" in {
      val result = cassandra.get(path + "first")
      result must equalTo(Some(jsmith("first")))
    }

  }

  "order preservation" should {

    // Ugly hack
    def pad(s : Int, length : Int) : String = {
      ("0" * (length - s.toString.length)) + s.toString
    }

    "work on columns in super column family" in {
      val superPath = ColumnParent[String]("Super1", "ordering-test")
      val randomizer = new scala.util.Random(10)
      cassandra.remove(superPath)
      val superData = (0 until 50).map(pad(_, 3) ->(randomizer.nextInt.toString)).toList

      superData.map { case(key, data) =>
        cassandra.insertSuper(superPath, Map("1" -> Map(key -> data)))
      }

      //cassandra.insertSuper(superPath, Map("1" -> superData))

      cassandra.get(superPath ++ "1").get.keys.toList must containInOrder(superData.map(_._1).toList)
    }

    "work on standard columns" in {
      val path = ColumnParent[String]("Standard1", "ordering-test")
      cassandra.remove(path)
      val data = (0 until 25).map { i =>
        (('z' - i).toChar.toString -> i.toString)
      }.toList

      cassandra.insertNormal(path, data)
      cassandra.slice(path, None, None, Descending) must containInOrder(data)
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
      cassandra.slice(path, None, None, Ascending) must haveKey("age")
      cassandra.remove(path + "age")

      // Then: age column should not have value
      cassandra.get(path + "age") must beNone
    }

    "be able to remove entire row" in {
      cassandra.get(ColumnPath("Standard1", "jsmith-test", "first")) must beSomething
      // Given: John is removed from Cassandra
      cassandra.remove(ColumnParent[String]("Standard1", "jsmith-test"))
      // Then: It should not return anything
      cassandra.get(ColumnPath("Standard1", "jsmith-test", "first")) must beNone
    }
  }
}
