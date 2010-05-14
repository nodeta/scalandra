package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra._
import com.nodeta.scalandra.serializer.StringSerializer

import scala.util.Random

class ReadTest extends CassandraSpecification() {
  import ReadTest._
  import ReadTest.cassandra.{StandardSlice, SuperSlice, ColumnParent, ColumnPath}
  
  doBeforeSpec {
    cassandra("1", ColumnParent("Users", None)) = user
    tweets.foreach { case(id, data) =>
      cassandra(id, ColumnParent("Statuses", None)) = data
    }
    relationships.foreach { case(id, data) =>
      cassandra(id, ColumnParent("UserRelationships", None)) = data
    }
    Thread.sleep(200)
  }

  doAfterSpec {
    cassandra.remove("1", ColumnParent("Users", None))
    tweets.foreach { case(id, data) =>
      cassandra.remove(id, ColumnParent("Statuses", None))
    }
    relationships.foreach { case(id, data) =>
      cassandra.remove(id, ColumnParent("UserRelationships", None))
    }
  }
  
  "column fetching" should {
    "be able to get single column" in {
      "from standard column family" in {
        cassandra.get("1", ColumnPath("Users", None, "display_name")) must beSome("lcat")
      }
      "from super column family" in {
        val data = relationships("1")("user_timeline").toList
        val (key, value) = data(Random.nextInt(data.size))
        cassandra.get("1", ColumnPath("UserRelationships", Some("user_timeline"), key)) must beSome(value)
      }
    }

    "be able to get single super column" in {
      cassandra.get("1", ColumnParent("UserRelationships", Some("user_timeline"))) must beSome(relationships("1")("user_timeline"))
    }

    "error handling" in {
      "return None when no record is found" in {
        cassandra.get("1", ColumnPath("UserRelationships", Some("user_timeline"), "fail")) must beNone
      }

      /*"throw an exception when using #apply" in {
        cassandra("1", ColumnPath("UserRelationships", Some("user_timeline"), "fail")) must throwA[NoSuchElementException]
      }*/
    }
  }

  "column slicing" should {
    // Paths for data
    val path = cassandra.ColumnParent("Users", None)
    val spath = cassandra.ColumnParent("UserRelationships", Some("user_timeline"))
    // Row key
    val key = "1"

    "work with columns" in {
      "using range" in {
        "in standard column family" in {
          val result = cassandra.get(key, path, StandardSlice(Range(Some("display_name" + 0.toChar), None, Ascending, 2)))
          result must eventually(haveSize(2))
          result.keySet must containAll(List("real_name", "password"))
        }
        "in super column family" in {
          val result = cassandra.get(key, spath, StandardSlice(Range(Some("0030"), Some("0021"), Descending, 20)))
          result must eventually(haveSize(10))
          result.values.map(_.toInt).toList must containAll(List(30, 29, 21))
        }
      }
      "using list" in {
        "in standard column family" in {
          val result = cassandra.get(key, path, StandardSlice(List("display_name", "real_name", "invalid")))
          result must eventually(haveSize(2))
          result.keySet must containAll(List("real_name", "display_name"))
        }
        "in super column family" in {
          val result = cassandra.get(key, spath, StandardSlice(List("1000", "0010", "0099")))
          result must eventually(haveSize(2))
          result.values.map(_.toInt).toList must containAll(List(10, 99))
        }
      }
    }
    "work with super columns" in {
      "using range" in {
        val result = cassandra.get(key, spath / None, SuperSlice(Range(None, Some("public_timeline" + 0.toChar), Ascending, 20)))
        result must eventually(haveSize(1))
        result.keySet must containAll(List("public_timeline"))
      }
      "using list" in {
        val result = cassandra.get(key, spath / None, SuperSlice(List("user_timeline", "public_timeline", "invalid_timeline")))
        result must eventually(haveSize(2))
        result.keySet must containAll(List("user_timeline", "public_timeline"))
      }
    }
  }

  "ordering" should {
    val key = "1"
    val path = ColumnParent("UserRelationships", None)

    "must be preserved within super columns" in {
      cassandra.get(key, path / Some("user_timeline")).get.toList must eventually(
        containInOrder(relationships("1")("user_timeline").toList.sort { case((k1, v1), (k2, v2)) =>
          v1.toInt < v2.toInt
        })
      )
    }

    "must be preserved when slicing" in {
      "super columns" in {
        cassandra.get(key, path, SuperSlice(Range[String](None, None, Ascending, 1000))).toList must eventually(
          containInOrder(relationships("1").toList.sort { case((k1, v1), (k2, v2)) =>
            k1 < k2
          })
        )
      }

      "standard columns" in {
        cassandra.get(key, path / Some("user_timeline"), StandardSlice(Range[String](None, None, Ascending, 1000))).toList must eventually(
          containInOrder(relationships("1")("user_timeline").toList.sort { case((k1, v1), (k2, v2)) =>
            k1 < k2
          })
        )
      }
    }
  }

  "counting" should {
    // Paths for data
    val path = cassandra.ColumnParent("Users", None)
    val spath = cassandra.ColumnParent("UserRelationships", None)
    // Row key
    val key = "1"

    "be able to count columns" in {
      "standard column family" in {
        cassandra.count(key, path) must eventually(equalTo(user.size))
      }
      "super column family" in {
        cassandra.count(key, spath / Some("user_timeline")) must eventually(equalTo(relationships("1")("user_timeline").size))
      }
    }
    "be able to count super columns" in {
      cassandra.count(key, spath) must eventually(equalTo(relationships("1").size))
    }
  }

  "multiget" should {
    val path = cassandra.ColumnParent("Statuses", None)
    "be able to multiget columns" in {
      cassandra.get(List("1", "2", "3"), path / "id").values.map(_.map(_._2)).toList must containAll(List("1", "2", "3").map(Some(_)))
    }

    "be able to multiget supercolumns" in {
      cassandra.get(List("1"), ColumnParent("UserRelationships", Some("user_timeline")))("1") must beSomething
    }
    
    "be able to slice multiple records" in {
      "super columns" in {
        cassandra.get(List("1"), ColumnParent("UserRelationships", None), SuperSlice(List("user_timeline", "public_timeline")))("1") must haveKey("user_timeline")
      }
      "standard columns" in {
        "super column family" in {
          cassandra.get(List("1"), ColumnParent("UserRelationships", Some("public_timeline")), StandardSlice(List("0020", "0030")))("1") must haveKey("0030")
        }
        "standard column family" in {
          cassandra.get(List("1", "2", "3"), ColumnParent("Statuses", None), StandardSlice(List("id"))) must containAll(
            List("1", "2", "3").map(i => (i -> Map("id" -> i)))
          )
        }
      }
    }
  }

  "range slicing" should {
    val path = ColumnParent("Statuses", None)

    "be able to list key ranges" in {
      val r = cassandra.get(path, StandardSlice(List("id")), Some("20"), Some("24"), 100)
      r must haveKey("20")
      r must haveKey("24")
    }

    "contain slice data" in {
      val r = cassandra.get(path, StandardSlice(List("id", "text")), Some("30"), Some("40"), 5)
      r must haveSize(5)
      r must haveKey("34")
      r("34") must haveKey("text")
    }
  }
}

object ReadTest {
  val connection = Connection(9162)
  val cassandra = new Client(connection, "Twitter", Serialization(StringSerializer, StringSerializer, StringSerializer), ConsistencyLevels.quorum)
  val userCount = 20
  
  val user : Map[String, String] = Map(
    "display_name" -> "lcat",
    "real_name" -> "Lolcat",
    "password" -> "secret"
  )

  lazy val tweets : Map[String, Map[String, String]] = {
    def tweet(id : Int) : (String, Map[String, String]) = {
      (id.toString -> Map(
        "id" -> id.toString,
        "text" -> "Tweet #%d".format(id),
        "user_id" -> "1"
      ))
    }

    Map() ++ (1 until 101).map(i => tweet(i))
  }

  lazy val relationships : Map[String, Map[String, Map[String, String]]] = {
    val data = Map() ++ tweets.map { case(id, data) =>
      ("%04d".format(id.toInt) -> id)
    }

    Map("1" -> Map("user_timeline" -> data, "public_timeline" -> data, "yet_another_timeline" -> data, "yet_another_timeline1" -> data))
  }
}
