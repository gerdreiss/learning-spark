package com.github.learningspark.udemy1

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}
import scala.math.sqrt

object MovieSimilarities {

  type UserID = Int
  type MovieID = Int
  type MovieName = String
  type Rating = Double

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[MovieID, MovieName] = {

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

  type MovieRating = (MovieID, Rating)
  type UserRatingPair = (UserID, (MovieRating, MovieRating))

  def makePairs(userRatings: UserRatingPair): ((MovieID, MovieID), (Rating, Rating)) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }

  type RatingPair = (Rating, Rating)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    (score, numPairs)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarities")

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings = sc.textFile("../ml-100k/u.data")
      .map(_.split("\t"))
      // extract      User ID           Movie ID         Movie Rating
      .map(fields => (fields(0).toInt, (fields(1).toInt, fields(2).toDouble)))

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratings.join(ratings)

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()

    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")

    // Extract similarities for the movie we care about that are "good".

    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurrenceThreshold = 50.0

      val movieID: MovieID = args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above     

      val filteredResults = moviePairSimilarities.filter {
        case ((movie1, movie2), (score, occurrence)) =>
          (movie1 == movieID || movie2 == movieID) &&
            score > scoreThreshold &&
            occurrence > coOccurrenceThreshold
      }

      // Sort by quality score.
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(ascending = false).take(10)

      println("\nLoading movie names...")
      val nameDict = loadMovieNames()

      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(f"${nameDict(similarMovieID).padTo(60, " ").mkString}score: ${sim._1}%18.17f\tstrength: ${sim._2}%4d")
      }
    }
  }
}