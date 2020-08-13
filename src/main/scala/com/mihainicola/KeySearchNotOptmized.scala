package com.mihainicola

import org.apache.spark.rdd._
import org.apache.spark.SparkContext

import com.mihainicola.storage._
import org.apache.livy.scalaapi.ScalaJobContext

object KeySearchNotOptimized
  extends SparkJob[Unit, Long, KeySearchParams] {

  override def appName: String = "Keyword search not optimized"

  override def fnDataLoad = (params: KeySearchParams, ctx: ScalaJobContext) => {
    // load the data and return the lines
    val wikipediaEntries = ctx.sc.textFile(params.inputLocation)

    // show total count (and trigger the loading into memory)
    println(s"Total lines in doc: ${wikipediaEntries.count()}")
  }

  override def fnCompute = (params: KeySearchParams, ctx: ScalaJobContext) => {
    val wikipediaEntriesAgain = ctx.sc.textFile(params.inputLocation)

      // get the results of the search
    val keyword = params.searchKey
    wikipediaEntriesAgain.filter(line => line.contains(keyword))
      .map(line => line.substring(0, 100) ++ "...")
      .map(line => (line, 1))
      .count()
  }

}
