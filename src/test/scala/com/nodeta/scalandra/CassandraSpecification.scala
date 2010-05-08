package com.nodeta.scalandra.tests

import org.specs.Specification

trait CassandraSpecification extends Specification {
  if (System.getProperty("cassandra.running") != "true") {
    EmbeddedServerHelper.setup
  }
}
