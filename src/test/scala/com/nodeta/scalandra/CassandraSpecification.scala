package com.nodeta.scalandra.tests

import org.specs.Specification
import org.apache.cassandra.thrift.{Mutation, ThriftGlue}

abstract class CassandraSpecification extends Specification {
  if (System.getProperty("cassandra.running") != "true") {
    EmbeddedServerHelper.setup
  }
}
