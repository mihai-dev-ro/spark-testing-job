package com.mihainicola

import java.io.{File, FileNotFoundException}
import java.net.URI

import com.mihainicola._
import org.apache.livy.LivyClientBuilder
import org.apache.livy.scalaapi._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

trait SparkJobSubmission[T,U,Params <: SparkJobParams] {

  def livyUrl: String

  def sparkJob: SparkJob[T, U, Params]

  val livyClient = new LivyScalaClient(
      new LivyClientBuilder().setURI(new URI(livyUrl)).build())

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
    livyClient, livyClientJarPath)

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
