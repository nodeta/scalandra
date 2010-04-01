package com.nodeta.scalandra

import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.ConsistencyLevel._

case class ConsistencyLevels(read : ConsistencyLevel, write : ConsistencyLevel) {}

object ConsistencyLevels extends (() => ConsistencyLevels) {
  lazy val default = { ConsistencyLevels(ONE, ZERO) }
  lazy val one = { ConsistencyLevels(ONE, ONE) }
  lazy val quorum = { ConsistencyLevels(QUORUM, QUORUM) }
  lazy val all = { ConsistencyLevels(ALL, ALL) }

  def apply() : ConsistencyLevels = default
}
