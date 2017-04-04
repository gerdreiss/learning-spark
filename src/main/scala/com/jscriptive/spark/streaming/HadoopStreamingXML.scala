package com.jscriptive.spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.xml.{Elem, XML}

case class Book(id: String, title: String, author: String, genre: String, price: Double, publishDate: String, description: String)

object HadoopStreamingXML extends App {

  val jobConf = new JobConf()
  jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
  jobConf.set("stream.recordreader.begin", "<book")
  jobConf.set("stream.recordreader.end", "</book>")
  jobConf.setInputFormat(classOf[org.apache.hadoop.streaming.StreamInputFormat])
  jobConf.setOutputKeyClass(classOf[org.apache.hadoop.io.Text])
  jobConf.setOutputValueClass(classOf[org.apache.hadoop.io.Text])
  FileInputFormat.addInputPath(jobConf, new Path("data/books.xml"))

  val sparkSession = SparkSession
    .builder
    .appName("HadoopStreamingXML")
    .master("local[*]")
    .getOrCreate()

  // Load documents (one per line).
  val documents: RDD[(Text, Text)] = sparkSession.sparkContext
    .hadoopRDD(jobConf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])


  //<book id="bk102">
  //  <author>Ralls, Kim</author>
  //  <title>Midnight Rain</title>
  //  <genre>Fantasy</genre>
  //  <price>5.95</price>
  //  <publish_date>2000-12-16</publish_date>
  //  <description>A former architect battles corporate zombies,
  //    an evil sorceress, and her own childhood to become queen
  //    of the world.
  //  </description>
  //</book>

  val texts = documents
    .map(_._1.toString.trim())
    .map { s =>
      val xml: Elem = XML.loadString(s)
      val id = (xml \ "@id").text
      val title = (xml \ "title").text
      val author = (xml \ "author").text
      val genre = (xml \ "genre").text
      val price = (xml \ "price").text.toDouble
      val publishDate = (xml \ "publish_date").text
      val description = (xml \ "description").text
      Book(id, title, author, genre, price, publishDate, description)
    }

  import sparkSession.implicits._

  texts.toDS().show()
}
