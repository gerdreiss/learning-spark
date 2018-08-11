package com.github.learningspark.udemy2

import java.util.regex.Pattern

object Utilities {
  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    Logger.getRootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter(): Unit = {
    import scala.io.Source

    Source.fromFile("twitter.txt").getLines
      .map(_.split(" "))
      .filter(_.length == 2)
      .foreach(fields => System.setProperty("twitter4j.oauth." + fields(0), fields(1)))
  }

  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referrer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referrer $agent"
    Pattern.compile(regex)
  }
}
