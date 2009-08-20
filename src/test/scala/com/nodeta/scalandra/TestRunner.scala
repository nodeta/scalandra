package com.nodeta.scalandra.tests

import org.specs.runner.SpecsFileRunner

object TestRunner extends SpecsFileRunner("src/test/scala/**/*.scala", ".*",
  System.getProperty("system", ".*"), System.getProperty("example", ".*"))
