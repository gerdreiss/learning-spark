package com.jscriptive.spark

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountBetter")

    // Load each line of my book into an RDD
    val input = sc.textFile("book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(_.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(_.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.countByValue()

    // Print the results
    wordCounts.foreach(println)
  }
}