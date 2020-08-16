package com.mihainicola

import scopt.OptionParser

// trait SparkJobParams {
//   def jobJarPath: String
// }
trait SparkJobParams

abstract class SparkJobParamsParser[T <: SparkJobParams](appName: String)
  extends OptionParser[T](appName) {
    head("scopt", "3.x")
  }

