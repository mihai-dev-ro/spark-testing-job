package com.mihainicola.sparkjobfull

class KeySearchJobSubmission(
  override val sparkProps: Map[String, String],
  override val livyUrl: String = "http://localhost:8998/"
) extends SparkJobSubmission[Long, (Array[(String, Int)], Boolean), KeySearchParams] {

  override val sparkJob: SparkJob[Long, (Array[(String, Int)], Boolean), KeySearchParams] = KeySearch
}
