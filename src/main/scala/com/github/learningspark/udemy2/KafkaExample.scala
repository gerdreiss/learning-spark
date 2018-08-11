package com.github.learningspark.udemy2

import Utilities._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaExample {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topics = Set("testLogs")
    // Create our Kafka stream, which will contain (topic,message) pairs. We tack a
    // map(_._2) at the end in order to only get the messages, which contain individual
    // lines of data.
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      .map(_._2)
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
    ssc.checkpoint("checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
