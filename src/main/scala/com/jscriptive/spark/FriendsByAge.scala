package com.jscriptive.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    type Age = Int
    type Name = String
    type NumOfFriends = Int

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("fakefriends.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val ageAndFriends: RDD[(Age, NumOfFriends)] = lines
      .map(_.split(","))
      .map(fields => (fields(2).toInt, fields(3).toInt))

    val nameAndFriends: RDD[(Name, NumOfFriends)] = lines
      .map(_.split(","))
      .map(fields => (fields(1), fields(3).toInt))

    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    val totalsByAge: RDD[(Age, (NumOfFriends, Int))] = ageAndFriends
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val totalsByName: RDD[(Name, (NumOfFriends, Int))] = nameAndFriends
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge: RDD[(Age, Int)] = totalsByAge.mapValues(x => x._1 / x._2)
    val averagesByName: RDD[(Name, Int)] = totalsByName.mapValues(x => x._1 / x._2)

    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val resultsByAge: Array[(Age, NumOfFriends)] = averagesByAge.collect()
    val resultsByName: Array[(Name, NumOfFriends)] = averagesByName.collect()

    // Sort and print the final results.
    resultsByAge.sorted.foreach(println)
    resultsByName.sorted.foreach(println)
  }
}
