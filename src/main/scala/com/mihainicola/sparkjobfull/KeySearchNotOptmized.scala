package com.mihainicola.sparkjobfull

import org.apache.spark.rdd._
import org.apache.spark.SparkContext

import org.apache.livy.scalaapi.ScalaJobContext

object KeySearchNotOptimized
  extends SparkJob[Long, Array[String], KeySearchParams] {

  override def appName: String = "Keyword search not optimized"

  override def fnDataLoad = (params: KeySearchParams, ctx: ScalaJobContext) => {
    // load the data and return the lines
    val textFiles = (1 to params.nbFiles)
      .toList
      .map(i => s"${params.inputRootFileLocation}_${i}")

    val wikipediaEntries = ctx.sc.textFile(textFiles.mkString(","))

    // show total count (and trigger the loading into memory)
    wikipediaEntries.count()
  }

  override def fnCompute = (params: KeySearchParams, ctx: ScalaJobContext) => {
    val textFiles = (1 to params.nbFiles)
      .toList
      .map(i => s"${params.inputRootFileLocation}_${i}")

    val wikipediaEntriesAgain = ctx.sc.textFile(textFiles.mkString(","))

    // get the results of the search
    // val keyword = params.searchKey
    // wikipediaEntries.filter(line => line.contains(keyword))
    //   .map(line => line.substring(0, 100) ++ "...")
    //   .map(line => (line, 1))
    //   .collect()

    val keyword = params.searchKey
    wikipediaEntriesAgain
      .filter(line => line.contains(keyword))
      .collect()
  }

}
