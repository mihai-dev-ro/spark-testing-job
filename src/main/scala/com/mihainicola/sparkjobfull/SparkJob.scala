package com.mihainicola.sparkjobfull

import org.apache.spark.rdd._
import org.apache.livy.scalaapi.ScalaJobContext

trait SparkJob[T, U, Params <: SparkJobParams] {

  def appName: String

  /***
   *
   * Splitting the job into two phases (sub-jobs),
   *   1. First for loading the data and bring it to the form of being used in
   *     processing
   *   2. Second for executing the  computations that are being handled
   *     in other jobs that share the same context
   */
  def fnDataLoad: (Params, ScalaJobContext) => T
  def fnCompute: (Params, ScalaJobContext) => U
}
