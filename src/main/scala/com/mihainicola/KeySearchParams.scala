package com.mihainicola

// import scopt.OptionParser

case class KeySearchParams(
  inputLocation: String = "unknown",
  searchKey: String = "N/A",
  resultsLocation: String = "results.txt",
  jobJarPath: String = "N/A")
  extends SparkJobParams

class KeySearchParamsParser extends SparkJobParamsParser[KeySearchParams](
  "key search params") {

  opt[String]('i', "input").required
    .action((value, arg) => {
      arg.copy(inputLocation = value)
    })
    .text("Input file is required")

  opt[String]('s', "search").required
    .action((value, arg) => {
      arg.copy(searchKey = value)
    })
    .text("Search term is required")

  opt[String]('o', "output").required
    .action((value, arg) => {
      arg.copy(resultsLocation = value)
    })
    .text("Results location is required")

  opt[String]('j', "job-jar").required
    .action((value, arg) => {
      arg.copy(resultsLocation = value)
    })
    .text("Job Jar is required")
}
