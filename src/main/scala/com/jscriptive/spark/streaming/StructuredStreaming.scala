package com.jscriptive.spark.streaming

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

import com.jscriptive.spark.streaming.Utilities._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object StructuredStreaming {

  // Case class defining structured data for a line of Apache access log data
  case class LogEntry(ip: String,
                      client: String,
                      user: String,
                      dateTime: String,
                      request: String,
                      status: String,
                      bytes: String,
                      referrer: String,
                      agent: String)

  val logPattern: Pattern = apacheLogPattern()
  val datePattern: Pattern = Pattern.compile("\\[(.*?) .+]")

  // Function to convert Apache log times to what Spark/SQL expects
  def parseDateField(field: String): Option[String] = {
    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = dateFormat.parse(dateString)
      val timestamp = new java.sql.Timestamp(date.getTime)
      Option(timestamp.toString)
    } else {
      None
    }
  }

  // Convert a raw line of Apache access log data to a structured LogEntry object (or None if line is corrupt)
  def parseLog(row: Row): Option[LogEntry] = {
    val matcher: Matcher = logPattern.matcher(row.getString(0))
    if (matcher.matches()) {
      Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      None
    }
  }

  def main(args: Array[String]) {
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      //.config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .getOrCreate()

    setupLogging()

    // Create a stream of text files dumped into the logs directory
    val rawData = spark.readStream.text("logs")

    // Must import spark.implicits for conversion to DataSet to work!
    import spark.implicits._

    // Convert our raw text into a DataSet of LogEntry rows, then just select the two columns we care about
    rawData.flatMap(parseLog).select("status", "dateTime")
      // Group by status code, with a one-hour window.
      .groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")
      // Start the streaming query, dumping results to the console. Use "complete" output mode because we are aggregating
      // (instead of "append").
      .writeStream.outputMode("complete").format("console").start()
      // Keep going until we're stopped.
      .awaitTermination()

    spark.stop()
  }
}