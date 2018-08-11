package com.github.learningspark.udemy2

import Utilities._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, Time}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.util.Try

/** Monitors a stream of Apache access logs on port 9999, and prints an alarm
  * if an excessive ratio of errors is encountered.
  */
object LogAlarmer {

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    val host: ScallopOption[String]      = opt[String](default = Some("127.0.0.1"))
    val port: ScallopOption[Int]         = opt[Int](default = Some(9999))
    val windowLength: ScallopOption[Int] = opt[Int](default = Some(300))
    val windowSlide: ScallopOption[Int]  = opt[Int](default = Some(1))
    val maxIdle: ScallopOption[Long]     = opt[Long](default = Some(1800))
    val maxRatio: ScallopOption[Double]  = opt[Double](default = Some(0.5))
    verify()
  }

  def main(args: Array[String]) {

    val cliConf: Conf     = new Conf(args)
    val host: String      = cliConf.host()
    val port: Int         = cliConf.port()
    val windowLength: Int = cliConf.windowLength()
    val windowSlide: Int  = cliConf.windowSlide()
    val maxIdle: Duration = Duration(cliConf.maxIdle() * 1000L)
    val maxRatio: Double  = cliConf.maxRatio()


    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogAlarmer", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    //// Create a socket stream to read log data published via netcat on port 9999 locally
    //val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    //// Extract the status field from each log line
    //val statuses = lines.map(x => {
    //  val matcher: Matcher = pattern.matcher(x);
    //  if (matcher.matches()) matcher.group(6) else "[error]"
    //})
    //// Now map these status results to success and failure
    //val successFailure = statuses.map(x => {
    //  val statusCode = util.Try(x.toInt) getOrElse 0
    //  if (statusCode >= 200 && statusCode < 300) {
    //    "Success"
    //  } else if (statusCode >= 500 && statusCode < 600) {
    //    "Failure"
    //  } else {
    //    "Other"
    //  }
    //})
    //// Tally up statuses over a 5-minute window sliding every second
    //val statusCounts = successFailure.countByValueAndWindow(Seconds(300), Seconds(1))

    var t0 = Time(System.currentTimeMillis())

    // Better version of the above
    ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
      .map(pattern.matcher(_))
      .filter(_.matches())
      .map(_.group(6))
      .map(_.toInt)
      .map(status => {
        if (status >= 200 && status < 300) "Success"
        else if (status >= 500 && status < 600) "Failure"
        else "Other"
      })
      .countByValueAndWindow(Seconds(windowLength), Seconds(windowSlide))
      // For each batch, get the RDD's representing data from our current window
      .foreachRDD((rdd, now) => {

      // Keep track of total success and error codes from each RDD
      // var totalSuccess: Long = 0
      // var totalError: Long = 0
      //if (rdd.count() > 0) {
      //  val elements: Array[(String, Long)] = rdd.collect()
      //  for (element <- elements) {
      //    val result = element._1
      //    val count = element._2
      //    if (result == "Success") {
      //      totalSuccess += count
      //    }
      //    if (result == "Failure") {
      //      totalError += count
      //    }
      //  }
      //}

      // Better way to compute the totals
      val res: Map[String, Long] = rdd.reduceByKey(_ + _).collect().toMap

      val totalSuccess: Long = if (res.isEmpty) 0 else res("Success")
      val totalError: Long = if (res.isEmpty) 0 else res("Failure")

      // Print totals from current window
      println("Total success: " + totalSuccess + " Total failure: " + totalError)


      // Don't alarm unless we have some minimum amount of data to work with
      if (totalError + totalSuccess == 0) {
        val idle: Duration = now - t0
        if (idle > maxIdle) {
          println("Wake somebody up! The server seems down.")
        }
      } else {
        t0 = now
        if (totalError + totalSuccess > 100) {
          // Compute the error rate
          // Note use of util.Try to handle potential divide by zero exception
          val ratio: Double = Try(totalError.toDouble / totalSuccess.toDouble) getOrElse 1.0
          // If there are more errors than successes, wake someone up
          if (ratio > maxRatio) {
            // In real life, you'd use JavaMail or Scala's courier library to send an
            // email that causes somebody's phone to make annoying noises, and you'd
            // make sure these alarms are only sent at most every half hour or something.
            println("Wake somebody up! Something is horribly wrong.")
          } else {
            println("All systems go.")
          }
        }
      }
    })

    // Also in real life, you'd need to monitor the case of your site freezing entirely
    // and traffic stopping. In other words, don't use this script to monitor a real
    // production website! There's more you need.

    // Kick it off
    ssc.checkpoint("Tweets/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
