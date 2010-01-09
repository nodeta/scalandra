package com.nodeta.scalandra

import org.apache.cassandra.service.ConsistencyLevel._

case class ConsistencyLevels(read : Int, write : Int) {}

object ConsistencyLevels extends (() => ConsistencyLevels) {
  lazy val default = { ConsistencyLevels(ONE, ZERO) }
  lazy val one = { ConsistencyLevels(ONE, ONE) }
  lazy val quorum = { ConsistencyLevels(QUORUM, QUORUM) }
  lazy val all = { ConsistencyLevels(ALL, ALL) }

  def apply() : ConsistencyLevels = default
}
