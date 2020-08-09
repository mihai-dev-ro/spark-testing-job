package com.mihainicola

import org.apache.spark.rdd._
import org.apache.livy.scalaapi.ScalaJobContext

trait SparkJob[T, U, Params <: SparkJobParams] {

  def appName: String

  def getDataLoadPhaseInJob(
    params: Params): DataLoadPhaseInSparkJob[T, Params]

  def getComputePhaseInSparkJob(
    params: Params, rdd: RDD[T]): ComputePhaseInSparkJob[T, U, Params]
}

/**
  *
  * Splitting the job into two phases (sub-jobs),
  *   1. First for loading the data and bring it to the form of being used in
  *     processing
  *   2. Second for executing the  computations that are being handled
  *     in other jobs that share the same context
  *
  * @constructor Create the 1st Sub-Job that loads data into RDDs
  * @param jobParams The Job Specific Params like input location, serach term,
  *   etc
  */
abstract class DataLoadPhaseInSparkJob[T, Params <: SparkJobParams](
  val jobParams: Params) {

  def run(ctx: ScalaJobContext): RDD[T]
}

/**
  *
  * Splitting the job into two phases (sub-jobs),
  *   1. First for loading the data and bring it to the form of being used in
  *     processing
  *   2. Second for executing the  computations that are being handled
  *     in other jobs that share the same context
  *
  * @constructor Create the 2nd Sub-Job that executes the computation against
  *   the preloaded data
  * @param jobParams The Job Specific Params like input location, serach term,
  *   etc
  */
abstract class ComputePhaseInSparkJob[T, U, Params <: SparkJobParams](
  val jobParams: Params, var rdd: RDD[T]) {

  def run(ctx: ScalaJobContext): U
}
