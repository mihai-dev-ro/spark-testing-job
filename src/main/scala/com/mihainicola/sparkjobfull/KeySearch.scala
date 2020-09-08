package com.mihainicola.sparkjobfull

import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.livy.scalaapi.ScalaJobContext
import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.sql._
import scala.annotation.meta.param

object KeySearch extends SparkJob[Long, (Array[(String, Int)], Boolean), KeySearchParams] {

  override def appName: String = "Keyword search"

  override def fnDataLoad = (params: KeySearchParams, ctx: ScalaJobContext) => {

    // val mySession = ctx.sparkSession[SparkSession]
    // import mySession.implicits._

    // // load the data and return the lines
    // val wikipediaEntriesDF = ctx.sparkSession[SparkSession]
    //   .read
    //   .option("header", "false")
    //   .option("charset", "UTF8")
    //   .text(params.inputLocation)
    // // wikipediaEntries.cache()
    // ctx.setSharedVariable[DataFrame]("wikipediaEntries", wikipediaEntriesDF)
    // Register the DataFrame as a temporary view
    // wikipediaEntriesDF.createOrReplaceTempView("wikipediaEntries")

    val textFiles = (1 to params.nbFiles)
      .toList
      .map(i => s"${params.inputRootFileLocation}_${i}")

    val wikipediaEntries = textFiles
      .map { filePath =>
        ctx.sc.textFile(filePath).map(line => (filePath, line))
      }
      .reduce(_ union _)
    wikipediaEntries.cache()

    //val wikipediaEntries = ctx.sc.wholeTextFiles(textFiles.mkString(","))
    ctx.setSharedVariable[RDD[(String, String)]]("wikipediaEntries", wikipediaEntries)

    // show total count (and trigger the loading into memory)
    // wikipediaEntries.count()
    0
  }

  override def fnCompute = (params: KeySearchParams, ctx: ScalaJobContext) => {
    // val mySession = ctx.sparkSession[SparkSession]
    // import mySession.implicits._

    // val wikipediaEntriesDF = ctx.getSharedVariable[DataFrame]("wikipediaEntries")

    // val stageMetrics = StageMetrics(ctx.sparkSession)

    // // save to results location and return to the caller
    // val keyword = params.searchKey
    // // val output = stageMetrics.runAndMeasure {
    // //   // val results = wikipediaEntries.filter(line => line.contains(keyword))
    // //   // results.saveAsTextFile(params.resultsLocation)
    // //   // results.collect()

    // //   wikipediaEntriesDF.filter($"value".rlike(params.searchKey))
    // // }
    // val output = wikipediaEntriesDF.filter($"value".rlike(params.searchKey))
    // // val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
    // // stageMetrics.saveData(aggregatedDF, s"${params.resultsLocation}_perf_report")

    // val mySession = ctx.sparkSession[SparkSession]
    // import mySession.implicits._
    // val stageMetrics = StageMetrics(mySession)

    val wikipediaEntries = ctx.getSharedVariable[RDD[(String, String)]]("wikipediaEntries")
    val searchKey = params.searchKey
    val isCached = wikipediaEntries.getStorageLevel.useMemory

    //val output = stageMetrics.runAndMeasure {
    val output = wikipediaEntries
      .filter(x => x._2.indexOf(searchKey) >= 0)
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .collect()
    // }
    // output.saveAsTextFile(params.resultsLocation)
    (output, isCached)
  }

}
