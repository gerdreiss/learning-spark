package com.jscriptive.spark

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountAndLength {

  case class Numbers(count: Int, length: Int)

  case class Word(word: String, numbers: Numbers) {
    override def toString: String = {
      val paddedWord: String = word.padTo(18, " ").map(_.toString).mkString
      f"$paddedWord%s ${numbers.length}%6d ${numbers.count}%4d"
    }
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")

    sc.textFile("data/book.txt")
      .flatMap(_.split("\\W+"))
      .map(_.toLowerCase())
      .map(word => (word, Numbers(1, word.length)))
      .reduceByKey((nums1, nums2) => Numbers(nums1.count + nums2.count, nums1.length))
      .map {
        case (word, nums) => (nums.count, Word(word, nums))
      }
      .sortByKey()
      .foreach(result => println(s"${result._2}"))
  }
}
