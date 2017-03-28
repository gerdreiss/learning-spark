name := "learning-spark"

version := "1.0"

organization := "com.jscriptive"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.1.0"

  Seq(
    // Apache Log4j
    "log4j" % "log4j" % "1.2.14",
    // Apache Spark
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    // Config
    "com.typesafe" % "config" % "1.3.1",
    // ScalaTest
    "org.scalatest" %% "scalatest" % "3.0.1" % Test
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

