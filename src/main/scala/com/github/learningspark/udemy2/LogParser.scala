package com.github.learningspark.udemy2

import Utilities._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Maintains top URL's visited over a 5 minute window, from a stream
  * of Apache access logs on port 9999.
  */
object LogParser {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    //ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    //  // Extract the request field from each log line
    //  .map(line => {
    //    val matcher: Matcher = pattern.matcher(line)
    //    if (matcher.matches()) matcher.group(5)
    //  })
    //  // Extract the URL from the request
    //  .map(line => {
    //    val arr = line.toString.split(" ")
    //    if (arr.size == 3) arr(1) else "[error]"
    //  })
    //  // Reduce by URL over a 5-minute window sliding every second
    //  .map((_, 1))
    //  .reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    //  // Sort and print the results
    //  .transform(rdd => rdd.sortBy(_._2, ascending = false))
    //  .print()

    // Better way to do it
    // Create a socket stream to read log data published via netcat on port 9999 locally
    ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
      // get a pattern matcher for each line
      .map(pattern.matcher(_))
      // filter for those lines that match the pattern
      .filter(_.matches())
      // get the requests
      .map(_.group(5))
      // split the request string into parts GET /request HTTP 1.0
      .map(_.split(" "))
      // filter for those that have the three parts present
      .filter(_.length == 3)
      // get the URL
      .map(_(1))
      // map to URL, count tuple
      .map((_, 1))
      // count the occurrence of each URL
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
      // sort the RDD by count descending
      .transform(_.sortBy(_._2, ascending = false))
      // print the first 10 lines
      .print()

    // Kick it off
    ssc.checkpoint("Tweets/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
