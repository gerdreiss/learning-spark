name := "learning-spark"

version := "1.0"

organization := "com.jscriptive"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.1.0"
  val scallopVersion = "2.1.1"
  val hadoopVersion = "2.8.0"

  Seq(
    // Apache Spark
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-flume" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-flume-sink" % sparkVersion,
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.1",
    "com.databricks" %% "spark-xml" % "0.4.1",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-streaming" % hadoopVersion,
    "org.rogach" %% "scallop" % scallopVersion,
    // Config
    "com.typesafe" % "config" % "1.3.1"
  )
}

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  "-target:jvm-1.8",
  "-encoding", "UTF-8"
)

exportJars := true

crossPaths := false
