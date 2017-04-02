name := "learning-spark"

version := "1.0"

organization := "com.jscriptive"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.1.0"
  val scallopVersion = "2.1.1"

  Seq(
    // Apache Spark
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
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
