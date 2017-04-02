package com.jscriptive.spark.streaming

import com.jscriptive.spark.streaming.Utilities._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

/** Illustrates using SparkSQL with Spark Streaming, to issue queries on 
  * Apache log data extracted from a stream on port 9999.
  */
object LogSQL {

  /** Case class for converting RDD to DataFrame */
  case class Record(url: String, status: Int, agent: String)

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val conf = new SparkConf()
      .setAppName("LogSQL")
      .setMaster("local[*]")
      .set("spark.sql.warehouse.dir", "data")
    val ssc = new StreamingContext(conf, Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    //val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    //// Extract the (URL, status, user agent) we want from each log line
    //val requests = lines.map(x => {
    //  val matcher: Matcher = pattern.matcher(x)
    //  if (matcher.matches()) {
    //    val request = matcher.group(5)
    //    val requestFields = request.toString.split(" ")
    //    val url = util.Try(requestFields(1)) getOrElse "[error]"
    //    (url, matcher.group(6).toInt, matcher.group(9))
    //  } else {
    //    ("error", 0, "error")
    //  }
    //})

    ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
      .map(pattern.matcher(_))
      .filter(_.matches())
      .map(matcher => {
        val url = Try(matcher.group(5).split(" ")(1)) getOrElse "[error]"
        val status = matcher.group(6).toInt
        val agent = matcher.group(9)
        // SparkSQL can automatically create DataFrames from Scala "case classes".
        // We created the Record case class for this purpose.
        // So we'll convert each RDD of tuple data into an RDD of "Record"
        // objects, which in turn we can convert to a DataFrame using toDF()
        Record(url, status, agent)
      })
      // Process each RDD from each batch as it comes in
      .foreachRDD((rdd, time) => {
      // So we'll demonstrate using SparkSQL in order to query each RDD
      // using SQL queries.

      // Get the singleton instance of SQLContext
      val sparkSession = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local[*]")
        .getOrCreate()

      import sparkSession.implicits._

      // Create a SQL table from this DataFrame
      rdd.toDF().createOrReplaceTempView("requests")

      // Count up occurrences of each user agent in this RDD and print the results.
      // The powerful thing is that you can do any SQL you want here!
      // But remember it's only querying the data in this RDD, from this batch.
      val wordCountsDataFrame = sparkSession.sql("select agent, count(*) as total from requests group by agent")
      println(s"========= $time =========")
      wordCountsDataFrame.show()

      // If you want to dump data into an external database instead, check out the
      // org.apache.spark.sql.DataFrameWriter class! It can write dataframes via
      // jdbc and many other formats! You can use the "append" save mode to keep
      // adding data from each batch.
    })

    // Kick it off
    // ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
