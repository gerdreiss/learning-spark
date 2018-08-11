package com.github.learningspark.udemy2

import java.util.concurrent.atomic._

import Utilities._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/** Uses thread-safe counters to keep track of the average length of
  * Tweets in a stream.
  */
object AverageTweetLength {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "AverageTweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(_.getText)

    // Map this to tweet character lengths.
    val lengths = statuses.map(_.length)

    // As we could have multiple processes adding into these running totals
    // at the same time, we'll just Java's AtomicLong class to make sure
    // these counters are thread-safe.
    val totalTweets = new AtomicLong(0)
    val totalChars = new AtomicLong(0)

    // In Spark 1.6+, you  might also look into the mapWithState function, which allows
    // you to safely and efficiently keep track of global state with key/value pairs.
    // We'll do that later in the course.
    println("---------------------------------------------")
    println("| Total tweets | Total characters | Average |")
    println("---------------------------------------------")
    lengths.foreachRDD((rdd, _) => {

      val count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)

        totalChars.getAndAdd(rdd.reduce(_ + _))

        println(f"| ${totalTweets.get()}%12d | ${totalChars.get()}%16d | ${totalChars.get() / totalTweets.get()}%7d |")
      }
    })

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("Tweets/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
