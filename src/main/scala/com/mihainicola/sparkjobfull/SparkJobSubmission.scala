package com.mihainicola.sparkjobfull

import java.io.{File, FileNotFoundException}
import java.net.URI
import scala.collection.JavaConversions._


import com.mihainicola._
import org.apache.livy.LivyClientBuilder
import org.apache.livy.scalaapi._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

trait SparkJobSubmission[T,U,Params <: SparkJobParams] {

  def livyUrl: String

  def sparkProps: Map[String, String]

  def sparkJob: SparkJob[T, U, Params]

  // val livyClient = new LivyScalaClient(
  //   new LivyClientBuilder()
  //     .setURI(new URI(livyUrl))
  //     .setConf("spark.mesos.containerizer", "mesos")
  //     .setConf("spark.shuffle.service.enabled", "true")
  //     .setConf("spark.dynamicAllocation.enabled", "true")
  //     .setConf("spark.dynamicAllocation.minExecutors", "2")
  //     .setConf("spark.dynamicAllocation.maxExecutors", "10")
  //     .setConf("spark.local.dir", "/tmp/spark")
  //     .setConf("spark.mesos.executor.docker.volumes", "/var/lib/tmp/spark:/tmp/spark:rw")
  //     .setConf("spark.eventLog.enabled", "true")
  //     .setConf("spark.eventLog.dir", "hdfs://hdfs/history")
  //     .setConf("spark.executor.memory", "1g")
  //     .setConf("spark.executor.cores", "1")
  //     .setConf("spark.driver.bindAddress", "0.0.0.0")
  //     .setConf("spark.driver.port", "7001")
  //     .setConf("spark.blockManager.port", "7005")
  //     .setConf("spark.mesos.executor.docker.image", "mesosphere/spark:2.11.0-2.4.6-scala-2.11-hadoop-2.9")
  //     .setConf("spark.mesos.executor.home","/opt/spark")
  //     .setConf("spark.mesos.uris", "http://api.hdfs.marathon.l4lb.thisdcos.directory/v1/endpoints/hdfs-site.xml,http://api.hdfs.marathon.l4lb.thisdcos.directory/v1/endpoints/core-site.xml")
  //     .setConf("spark.metrics.conf.*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink")
  //     .setConf("spark.metrics.conf.*.sink.graphite.host", "spark-graphite.marathon.l4lb.thisdcos.directory")
  //     .setConf("spark.metrics.conf.*.sink.graphite.port", "2003")
  //     .setConf("spark.metrics.conf.*.sink.graphite.period", "10")
  //     .setConf("spark.metrics.conf.*.sink.graphite.unit", "seconds")
  //     .setConf("spark.metrics.conf.*.sink.graphite.prefix", "spark_job_test")
  //     .setConf("spark.metrics.conf.*.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
  //     .setConf("spark.extraListeners", "ch.cern.sparkmeasure.InfluxDBSink")
  //     .setConf("spark.sparkmeasure.influxdbURL", "http://spark-influxdb.marathon.l4lb.thisdcos.directory:8086")
  //     .build()
  // )

  val livyClient = new LivyScalaClient(
    new LivyClientBuilder()
      .setURI(new URI(livyUrl))
      .setConf("spark.mesos.containerizer", "mesos")
      .setConf("spark.shuffle.service.enabled", "true")
      .setConf("spark.dynamicAllocation.enabled", "true")
      .setConf("spark.dynamicAllocation.minExecutors", "3")
      .setConf("spark.dynamicAllocation.maxExecutors", "10")
      .setConf("spark.local.dir", "/tmp/spark")
      .setConf("spark.mesos.executor.docker.volumes", "/var/lib/tmp/spark:/tmp/spark:rw")
      .setConf("spark.eventLog.enabled", "true")
      .setConf("spark.eventLog.dir", "hdfs://hdfs/history")
      .setConf("spark.executor.memory", "2g")
      .setConf("spark.executor.cores", "1")
      .setConf("spark.driver.bindAddress", "0.0.0.0")
      .setConf("spark.driver.port", "7001")
      .setConf("spark.blockManager.port", "7005")
      .setConf("spark.mesos.executor.docker.image", "mesosphere/spark:2.11.0-2.4.6-scala-2.11-hadoop-2.9")
      .setConf("spark.mesos.executor.home","/opt/spark")
      .setConf("spark.mesos.uris", "http://api.hdfs.marathon.l4lb.thisdcos.directory/v1/endpoints/hdfs-site.xml,http://api.hdfs.marathon.l4lb.thisdcos.directory/v1/endpoints/core-site.xml")
      .setAll(sparkProps)
      .build()
  )

  val livyClientJarPath = getSourcePath(livyClient)

  /**
   *  Uploads the Scala-API Jar and the examples Jar from the target directory.
   *  @throws FileNotFoundException If either of Scala-API Jar or examples Jar is not found.
   */
  @throws(classOf[FileNotFoundException])
  private def uploadRelevantJarsForSparkJob(
     livyClient: LivyScalaClient,
     livyApiClientJarPath: String) = {

    // val thisJarPath = "/Volumes/Playground/cloud/" +
    //   "__CAREER/Master/LucrareDiploma/__ProiectLicenta/" +
    //   "kyme-scheduling-service/target/scala-2.11/" +
    //   "kyme-scheduling-service-assembly-0.0.1-SNAPSHOT.jar"

    val thisJarPath = getSourcePath(this)

    for {
      _ <- uploadJar(livyClient, livyApiClientJarPath)
      _ <- uploadJar(livyClient, thisJarPath)
    } yield ()
  }

  /**
   *  Adds the Scala-API Jar and the examples Jar as URIs accessible from
   *  cluster nodes.
   *  @throws FileNotFoundException If either of Scala-API Jar or examples Jar is not found.
   */
  @throws(classOf[FileNotFoundException])
  private def addRelevantJarsForSparkJob(
    livyClient: LivyScalaClient,
    livyClientJarUri: String,
    appJarUri: String) = {


    for {
      _ <- livyClient.addJar(new URI(livyClientJarUri))
      _ <- livyClient.addJar(new URI(appJarUri))
    } yield ()
  }

  @throws(classOf[FileNotFoundException])
  private def getSourcePath(obj: Object): String = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null && source.getLocation.getPath != "") {
      source.getLocation.getPath
    } else {
      throw new FileNotFoundException(
        s"Jar containing ${obj.getClass.getName} not found.")
    }
  }

  /**
   * Upload a local jar file to be added to Spark application classpath
   *
   * @param livyClient The LivyScalaClient used to communicate with Livy Server
   * @param path Local path for the jar file
   */
  private def uploadJar(livyClient: LivyScalaClient, path: String) = {
    val file = new File(path)
//    val uploadJarFuture = livyClient.uploadJar(file)
//    Await.result(uploadJarFuture, 40 second) match {
//      case null => println("Successfully uploaded " + file.getName)
//    }
    livyClient.uploadJar(file)
  }

  /**
   * Upload a jar file located on network to be added to
   * Spark application classpath.
   * It must be reachable by the Spark driver process, can eb hdfs://, s3://,
   * or http://
   *
   * @param livyClient The LivyScalaClient used to communicate with Livy Server
   * @param uri Location of the jar
   */
  private def addJar(livyClient: LivyScalaClient, uri: URI) = {
    //    val addJarFuture = livyClient.addJar(uri)
    //    Await.result(addJarFuture, 40 second) match {
    //      case null => println("Successfully added " + uri)
    //    }
    livyClient.addJar(uri)
  }

  def init(): Future[Unit] = uploadRelevantJarsForSparkJob(
    livyClient, livyClientJarPath
  )

  def stop(): Unit = livyClient.stop(true)

  def initForCluster(appJars: List[String]): Future[Unit] = {
    val futures = appJars.map(jarUri => livyClient.addJar(new URI(jarUri)))
    Future.fold(futures)(())((_, _) => ())
  }

  def submitDataLoadJob(params: Params): ScalaJobHandle[T] = {
    val fn = sparkJob.fnDataLoad

    livyClient.submit(context => {
      fn(params, context)
    })
  }

  def submitComputeJob(params: Params): ScalaJobHandle[U] = {
    val fn = sparkJob.fnCompute

    livyClient.submit( context => {
      fn(params, context)
    })
  }
}
