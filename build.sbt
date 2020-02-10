name := "scalaua2020"

version := "0.0.1"

organization := "com.slasch"

scalaVersion := "2.12.10"

resolvers ++= Seq(
  "Sonatype" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Confluent" at "https://packages.confluent.io/maven/",
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)

scalacOptions ++= Seq(
  "-Xmax-classfile-name", "128",
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfatal-warnings",
  "-Yno-adapted-args",
  "-Xfuture"
)

libraryDependencies ++= {
  val akkaVersion  = "2.6.0"
  val sparkVersion = "2.4.4"

  Seq(
    "org.apache.spark"           %% "spark-core"           % sparkVersion,
    // "org.apache.spark"           %% "spark-streaming"      % sparkVersion % Provided,
    "org.apache.spark"           %% "spark-sql"            % sparkVersion,
    // "com.typesafe.akka"          %% "akka-actor"           % akkaVersion,
    // "com.typesafe.akka"          %% "akka-stream"          % akkaVersion,
    // "com.typesafe.scala-logging" %% "scala-logging"        % "3.9.2"
  "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
  )
}
