package com.mihainicola

import org.apache.spark.rdd._
import org.apache.spark.SparkContext

import com.mihainicola.storage._
import org.apache.livy.scalaapi.ScalaJobContext

object KeySearch extends SparkJob[String, Unit, KeySearchParams] {

  override def appName: String = "Keyword search"

  override def getDataLoadPhaseInJob(params: KeySearchParams):
    DataLoadPhaseInSparkJob[String, KeySearchParams] =
      new DataLoadPhaseInSparkJob[String, KeySearchParams](params) {

        def run(ctx: ScalaJobContext): RDD[String] = {
          // load the data and return the lines
          val wikipediaEntries = ctx.sc.textFile(params.inputLocation)

          // pre-process the data
          // val separators : Array[Char] = "\n".toCharArray
          // val stopWords: Set[String] = Set("the")

          // pre-process the strings (split)
          wikipediaEntries
          //  .flatMap(_.split(separators).map(_.trim()))
          //  .filter(word => (word.length() > 0) && !stopWords.contains(word))
        }
    }

  override def getComputePhaseInSparkJob(
    params: KeySearchParams, rdd: RDD[String]):
    ComputePhaseInSparkJob[String, Unit, KeySearchParams] =
      new ComputePhaseInSparkJob[String, Unit, KeySearchParams](params, rdd) {

        def run(ctx: ScalaJobContext): Unit = {
          // get the results of the search
          val keyword = params.searchKey
          rdd.filter(line => line.contains(keyword))
            .map(line => line.substring(0, 100) ++ "...")
            .map(line => (line, 1))
            .saveAsTextFile(params.resultsLocation)
        }
      }
}
