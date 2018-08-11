package com.github.learningspark.udemy1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerOrders {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerOrders")

    // Load each line of the source data into an RDD
    val result = sc.textFile("data/customer-orders.csv")
      .map(line => line.split(","))
      .map(fields => (fields(0).toInt, fields(2).toFloat))
      .reduceByKey(_ + _)
      .map(data => (data._2, data._1))
      .sortByKey()
      .collect()

    result.foreach {
      case (sum, custId) => println(f"$custId%3d $sum%10.2f")
    }
  }
}
