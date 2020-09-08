package com.mihainicola.sparkjobfull


case class KeySearchParams(
  inputRootFileLocation: String = "unknown",
  nbFiles: Int = 1,
  searchKey: String = "N/A",
  resultsLocation: String = "results.txt",
  appJars: List[String] = List.empty)
  extends SparkJobParams

class KeySearchParamsParser extends SparkJobParamsParser[KeySearchParams](
  "key search params") {

  opt[String]('i', "input").required
    .action((value, arg) => {
      arg.copy(inputRootFileLocation = value)
    })
    .text("Input Root File is required")

  opt[Int]('n', "nb-files").required
    .action((value, arg) => {
      arg.copy(nbFiles = value)
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

  // opt[String]('j', "job-jar").required
  //   .action((value, arg) => {
  //     arg.copy(jobJarPath = value)
  //   })
  //   .text("Job Jar is required")
}
