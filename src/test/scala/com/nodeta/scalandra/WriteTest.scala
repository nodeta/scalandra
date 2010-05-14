package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra._
import com.nodeta.scalandra.serializer.StringSerializer

import scala.util.Random

class WriteTest extends CassandraSpecification {
  import WriteTest._
  import WriteTest.cassandra.{StandardSlice, SuperSlice, ColumnParent, ColumnPath}

  /* Data to be inserted */
  val tweet = Map(
    "id" -> Random.nextInt.toString,
    "text" -> "longcat is long",
    "user_id" -> Random.nextInt.toString,
    "in_reply_to" -> Random.nextInt.toString
  )

  // Location for data in standard CF
  val path = cassandra.ColumnParent("StatusAudits", None)
  // Location for data in super CF
  val superPath = cassandra.ColumnParent("UserRelationships", None)

  // Row key
  val key = tweet("id")

  // Index data, for SCF
  val index = Map(
    "user_relationships" -> Map(
        tweet("id") -> tweet("id"),
        "lol" -> "cat"
      ), 
    "something_else" -> Map("test" -> "data")
  )

  "insert" should {
    "be able to add and get data to a normal column family" in {
      // Given: John is inserted to Cassandra
      cassandra(key, path) = tweet

      // Then: It should still have its old values.
      def result = { cassandra.get(key, path, StandardSlice(Range[String](None, None, Ascending, 1000))) }
      result must equalTo(tweet).eventually
      println("insert")
    }

    "be able to add and get data to a super column family" in {
      // Given: Data is inserted to Cassandra
      cassandra(key, superPath) = index

      // Then: It should still have its old values.
      def result = { cassandra.get(key, superPath, SuperSlice(Range[String](None, None, Ascending, 1000))) }
      result must equalTo(index).eventually
    }

    "should be able to insert single value" in {
      val value = Random.nextInt.toString
      val path = cassandra.ColumnPath("StatusAudits", None, "Random")
      val key = "random-test"
      // Given: Value is inserted to Cassandra
      cassandra(key, path) = value

      // Then: It should be readable from Cassandra.
      cassandra.get(key, path) must beSomething.which(_ must equalTo(value)).eventually
    }
  }
  
  "remove" should {
    cassandra(key, path) = tweet
    "be able to remove single column" in {
      // Given that age column is defined and the column is removed
      cassandra.get(key, path, StandardSlice(Range[String](None, None, Ascending, 1000))) must haveKey("in_reply_to")
      cassandra.remove(key, path / "in_reply_to")

      // Then: age column should not have value
      cassandra.get(key, path / "in_reply_to") must eventually(beNone)
    }

    "be able to remove entire row" in {
      cassandra.get(key, path / "text") must beSomething
      // Given: Tweet is removed from Cassandra
      cassandra.remove(key, cassandra.ColumnParent("StatusAudits", None))
      // Then: It should not return anything
      cassandra.get(key, cassandra.ColumnPath("StatusAudits", None, "text")) must eventually(beNone)
    }
  }
}

object WriteTest {
  val connection = Connection(9162)
  val cassandra = new Client(connection, "Twitter", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)
  val userCount = 20
}
