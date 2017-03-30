package com.jscriptive.spark

import org.apache.log4j._
import org.apache.spark.sql._

object SparkSQL {

  case class Person(ID: Int, name: String, age: Int, numFriends: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val sparkSession = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val lines = sparkSession.sparkContext.textFile("data/fakefriends.csv")
    val people = lines
      .map(_.split(','))
      .map(fields => Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt))

    // Infer the schema, and register the DataSet as a table.
    import sparkSession.implicits._
    val schemaPeople = people.toDS

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")

    // SQL can be run over DataFrames that have been registered as a table
    sparkSession
      .sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
      .collect()
      .foreach(println)

    sparkSession.stop()
  }
}