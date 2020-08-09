ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "com.mihainicola"
ThisBuild / version := "0.0.1-SNAPSHOT"
ThisBuild / publishMavenStyle := true

val sparkVersion = "2.4.6"

lazy val root = (project in file(".")).
  settings(
    name := "spark-testing-job",

    // sparkComponents := Seq(),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,

    // coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark"    %% "spark-core"         % sparkVersion          % Provided,
      "com.github.scopt"    %% "scopt"              % "3.7.1"               % Compile,
      "org.apache.livy"     %  "livy-api"           % "0.7.0-incubating",
      "org.apache.livy"     %% "livy-scala-api"     % "0.7.0-incubating",

      "org.scalatest"       %% "scalatest"          % "3.0.1"               % Test
    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    pomIncludeRepository := { x => false },

    resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
