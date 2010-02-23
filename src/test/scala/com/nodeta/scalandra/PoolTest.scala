package com.nodeta.scalandra.tests

import org.specs._
import com.nodeta.scalandra.ConnectionProvider
import com.nodeta.scalandra.pool.StackPool

class PoolTest extends Specification {  
  "Connection pool" should {
    val pool = StackPool(ConnectionProvider("localhost", 9162))
    "expose connections to blocks of code" in {
      pool { connection =>
        connection.isOpen must equalTo(true)
        "foo"
      } must equalTo("foo")
    }
    "not create excess connections" in {
      pool { connection =>
        pool.active must equalTo(1)
      }
      
      pool.idle must equalTo(1)
    }
  }
}
