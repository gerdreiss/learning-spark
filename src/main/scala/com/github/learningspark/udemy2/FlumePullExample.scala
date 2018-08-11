package com.github.learningspark.udemy2

import Utilities._
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Example of connecting to Flume log data, in a "pull" configuration. */
object FlumePullExample {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "FlumePullExample", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // The only difference from the push example is that we use createPollingStream instead of createStream.
    FlumeUtils.createPollingStream(ssc, "localhost", 9092)
      // This creates a DStream of SparkFlumeEvent objects. We need to extract the actual messages.
      // This assumes they are just strings, like lines in a log file.
      // In addition to the body, a SparkFlumeEvent has a schema and header you can get as well. So you
      // could handle structured data if you want.
      .map(_.event.getBody.array())
      .map(new String(_))
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
      .map(_ (1))
      // map to URL, count tuple
      .map((_, 1))
      // count the occurrence of each URL
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
      // sort the RDD by count descending
      .transform(_.sortBy(_._2, ascending = false))
      // print the first 10 lines
      .print()

    // Kick it off
    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
