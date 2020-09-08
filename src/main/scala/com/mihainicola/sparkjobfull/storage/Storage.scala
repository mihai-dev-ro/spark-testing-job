package com.mihainicola.sparkjobfull.storage

import org.apache.spark.rdd._

trait Storage {
  def words: RDD[String]
  def writeToDisk(rdd: RDD[KeySearchShema]): Unit
}

class HDFSStorage(val inputLocation: String, val outputLocation: String) extends Storage {

  /**
    * Storage defines all input and output logic. How and where tables and files
    * should be read and saved
    */

  private def readFile(location: String): RDD[String] = ???

  override def words: RDD[String] = readFile(inputLocation)
  override def writeToDisk(ds: RDD[KeySearchShema]): Unit = {
    ds.saveAsTextFile(outputLocation)
  }
}

case class KeySearchShema(keyword: String, result: List[(String, Long)])
