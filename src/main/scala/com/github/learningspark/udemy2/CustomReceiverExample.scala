// Custom receiver docs at http://spark.apache.org/docs/latest/streaming-custom-receivers.html

package com.github.learningspark.udemy2

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import Utilities._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Example from the Spark documentation; this implements a socket
  * receiver from scratch using a custom Receiver.
  */
class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
      userInput = reader.readLine()
      while (!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}

/** Illustrates using a custom receiver to listen for Apache logs on port 7777
  * and keep track of the top URL's in the past 5 minutes.
  */
object CustomReceiverExample {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "CustomReceiverExample", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    ssc.receiverStream(new CustomReceiver("localhost", 7777))
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
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

