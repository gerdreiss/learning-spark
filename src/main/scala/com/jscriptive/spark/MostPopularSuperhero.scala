package com.jscriptive.spark

import org.apache.log4j._
import org.apache.spark._

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

    // Build up a hero ID -> name RDD
    val names = sc.textFile("data/marvel-names.txt")
    val namesRdd = names
      .map(_.split('\"'))
      .filter(_.length > 1)
      .map(fields => (fields(0).trim().toInt, fields(1)))
      .persist()

    // Load up the superhero co-appearance data
    val connectionsRdd = sc.textFile("data/marvel-graph.txt")
      .map(_.split("\\s+"))
      // Convert to (heroID, number of connections) RDD
      .map(elements => (elements.head.toInt, elements.tail.length))
      // Combine entries that span more than one line
      .reduceByKey(_ + _)
      .persist()

    // Find the max # of connections
    val mostPopular = connectionsRdd
      // Flip it to # of connections, hero ID
      .map { case (superheroID, connections) => (connections, superheroID) }
      .max()

    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2).head

    // Print out our answer!
    println()
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
    println()

    val top10 = connectionsRdd.join(namesRdd)
      .sortBy(row => row._2._1, ascending = false)
      .take(10)

    println()
    println("Top 10 Superheroes by co-appearances")
    println("Appearances |    ID | Name")
    println("------------------------------------------------")
    top10
      .map {
        case (id, (connections, name)) => f"$connections%11d |$id%6d | $name%s"
      }.foreach(println)
  }
}
