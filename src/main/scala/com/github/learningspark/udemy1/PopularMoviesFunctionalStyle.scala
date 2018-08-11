package com.github.learningspark.udemy1

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object PopularMoviesFunctionalStyle {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    Source.fromFile("../ml-100k/u.item").getLines()
      .map(_.split('|'))
      .filter(_.length > 1)
      .map(fields => fields(0).toInt -> fields(1))
      .toMap
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")

    // Create a broadcast variable of our ID -> movie name map
    val nameDict = sc.broadcast(loadMovieNames())

    // Read in each rating line
    sc.textFile("../ml-100k/u.data")
      // Map to (movieID, 1) tuples
      .map(line => (line.split("\t")(1).toInt, 1))
      // Count up all the 1's for each movie
      .reduceByKey(_ + _)
      // Flip (movieID, count) to (count, movieID)
      .map { case (movieID, count) => (count, movieID) }
      // Sort
      .sortByKey()
      // Fold in the movie names from the broadcast variable
      .map { case (count, movieID) => (nameDict.value(movieID), count) }
      // Collect and print results
      .collect()
      // Map results to printable strings
      .map { case (movie, count) => f"${movie.padTo(82, " ").mkString} $count%4d" }
      // Print strings
      .foreach(println)
  }
}
