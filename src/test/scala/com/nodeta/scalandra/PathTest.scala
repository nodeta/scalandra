package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra.serializer._

class PathTest extends Specification {  
  "Path" should {
    val _serializer = Serialization(StringSerializer, StringSerializer, StringSerializer)
    val path = new scalandra.Path[String, String] {
      val serializer = _serializer
      val columnFamily = "test"
    }
    
    "Path should create ColumnParent with only column family defined" in {
      val parent = path.toColumnParent
      parent.column_family must equalTo("test")
      parent.super_column must beNull
    }
    "Path should create ColumnPath with only column family defined" in {
      val parent = path.toColumnPath
      parent.column_family must equalTo("test")
      parent.super_column must beNull
      parent.column must beNull
    }
    
    "ColumnParent should not have column" in {
      val path = new ColumnParent[String, String] {
        val serializer = _serializer
        val columnFamily = "test"
        val superColumn = Some("test")
      }
      path.toColumnPath.column must beNull
    }
    
    "ColumnPath should have column" in {
      val path = new ColumnPath[String, String] {
        val serializer = _serializer
        val columnFamily = "test"
        val superColumn = Some("test")
        val column = "test"
      }
      path.toColumnPath.column must notBeNull
    }
  }
}
