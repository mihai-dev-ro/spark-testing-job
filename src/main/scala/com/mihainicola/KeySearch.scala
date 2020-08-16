package com.mihainicola

import org.apache.spark.rdd._
import org.apache.spark.SparkContext

import com.mihainicola.storage._
import org.apache.livy.scalaapi.ScalaJobContext

object KeySearch extends SparkJob[Long, Array[String], KeySearchParams] {

  override def appName: String = "Keyword search"

  override def fnDataLoad = (params: KeySearchParams, ctx: ScalaJobContext) => {
    // load the data and return the lines
    val wikipediaEntries = ctx.sc.textFile(params.inputLocation)
    // wikipediaEntries.cache()
    ctx.setSharedVariable[RDD[String]]("wikipediaEntries", wikipediaEntries)

    // show total count (and trigger the loading into memory)
    wikipediaEntries.count()
  }

  override def fnCompute = (params: KeySearchParams, ctx: ScalaJobContext) => {
    val wikipediaEntries = ctx.getSharedVariable[RDD[String]]("wikipediaEntries")

    // get the results of the search
    // val keyword = params.searchKey
    // wikipediaEntries.filter(line => line.contains(keyword))
    //   .map(line => line.substring(0, 100) ++ "...")
    //   .map(line => (line, 1))
    //   .collect()

    val keyword = params.searchKey
    wikipediaEntries
      .filter(line => line.contains(keyword))
      .collect()
  }

}
