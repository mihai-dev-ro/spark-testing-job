package com.mihainicola

class KeySearchJobSubmission(
  override val livyUrl: String = "http://localhost:8998/"
) extends SparkJobSubmission[Long, Array[String], KeySearchParams] {

  override val sparkJob: SparkJob[Long, Array[String], KeySearchParams] = KeySearch
}
