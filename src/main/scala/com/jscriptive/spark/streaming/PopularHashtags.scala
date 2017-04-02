package com.jscriptive.spark.streaming

import com.jscriptive.spark.streaming.Utilities._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/** Listens to a stream of Tweets and keeps track of the most popular
  * hashtags over a 5 minute window.
  */
object PopularHashtags {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    //TwitterUtils.createStream(ssc, None)
    //  // Now extract the text of each status update into DStreams using map()
    //  .map(_.getText)
    //  // Blow out each word into a new DStream
    //  .flatMap(_.split(" "))
    //  // Now eliminate anything that's not a hashtag
    //  .filter(_.startsWith("#"))

    // Do the same as above with HashtagEntities
    TwitterUtils.createStream(ssc, None)
      // Extract the hashtags from the HashtagEntities
      .flatMap(_.getHashtagEntities)
      // Extract the hashtag text
      .map(_.getText)
      // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
      .map((_, 1))
      // Now count them up over a 5 minute window sliding every one second
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
      // Sort the results by the count values
      .transform(rdd => rdd.sortBy(_._2, ascending = false))
      // Print the top 10
      .print


    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("Tweets/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
