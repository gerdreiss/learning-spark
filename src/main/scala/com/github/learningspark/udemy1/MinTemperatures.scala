package com.github.learningspark.udemy1

import org.apache.log4j._
import org.apache.spark._

import scala.math.min

/** Find the minimum temperature by weather station */
object MinTemperatures {

  case class TempData(stationID: String, entryType: String, temperature: Float)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("data/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(_.split(",")).map(fields => TempData(fields(0), fields(2), fields(3).toFloat / 10f))

    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(data => data.entryType == "TMIN")

    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map(data => (data.stationID, data.temperature))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey((x, y) => min(x, y))

    // Collect, format, and print the results
    val results = minTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f C"
      println(s"$station minimum temperature: $formattedTemp")
    }
  }
}