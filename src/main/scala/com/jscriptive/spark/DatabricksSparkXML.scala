package com.jscriptive.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DatabricksSparkXML extends App {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  //val conf = new SparkConf()
  //  .setAppName("SparkXML")
  //  .setMaster("local[*]")
  //val ssc = new StreamingContext(conf, Seconds(1))

  val sparkSession = SparkSession.builder
    .appName("SparkXML")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  val df = sparkSession.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .load("data/books.xml")

  df.printSchema()
  df.show()
}
