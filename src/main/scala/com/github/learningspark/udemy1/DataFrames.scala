package com.github.learningspark.udemy1

import org.apache.log4j._
import org.apache.spark.sql._

object DataFrames {

  case class Person(ID: Int, name: String, age: Int, numFriends: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val lines = spark.sparkContext.textFile("data/fakefriends.csv")
    val people = lines
      .map(_.split(','))
      .map(fields => Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt))
      .toDS().cache()

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")

    println("Here is our inferred schema:")
    people.printSchema()

    println("Let's select the name column:")
    people.select("name").show()

    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()

    println("Group by age:")
    people.groupBy("age").count().show()

    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()

    spark.stop()
  }
}