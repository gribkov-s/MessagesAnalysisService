package ru.otus.sgribkov.messages

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import scala.collection.mutable.ArrayBuffer

object FromHDFStoDB extends App with DateTimeFunctions {

  val beginDate = "2021-03-23"
  val endDate = "2021-03-24"
  val hdfsURI = "hdfs://localhost:9000"
  val hdfsSourceFolder = hdfsURI + "/data/messages/details"
  val hdfsSourcePartitionFolder = "/_time_key="
  val jdbcConnection = Map(
    "url" -> "jdbc:postgresql://localhost:5432/postgres",
    "dbtable" -> "public.daily_users_activity",
    "user" -> "docker",
    "password" -> "docker",
    "driver" -> "org.postgresql.Driver"
  )

  val beginTimestampUnix = dateToUnixTimestamp(beginDate)
  val endTimestampUnix = dateToUnixTimestamp(endDate) + 86399
  //--------------------------------------

  val spark = SparkSession.builder()
    .appName("FromHDFStoDB")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._
  //--------------------------------------

  val conf = new Configuration()
  val fs = FileSystem.get(new URI(hdfsURI), conf)
  val partitionsStatuses = fs.listStatus(new Path(hdfsSourceFolder))
  var partitionsPaths = ArrayBuffer[String]()

  partitionsStatuses.foreach(p => {
    partitionsPaths = partitionsPaths :+ p.getPath.toString
  })

  val partitionsPathsByTimeRange = partitionsPaths
    .filter(_.contains(hdfsSourcePartitionFolder))
    .filter(p => {
      val partition = p.replace(hdfsSourceFolder + hdfsSourcePartitionFolder, "").toInt
      partition >= beginTimestampUnix && partition <= endTimestampUnix
    })

  //partitionsPathsByTimeRange.foreach(println)
  //--------------------------------------

  if (partitionsPathsByTimeRange.length > 1) {
    val messagesDetails = spark.read
      .format("parquet")
      .load(partitionsPathsByTimeRange: _*)

    val dailyUsersActivity = messagesDetails
      .withColumn("date", to_date($"time"))
      .withColumn("length", length($"content"))
      .withColumn("tags_num", size($"tags"))
      .groupBy($"date", $"originator")
      .agg(
        count("id").as("messages_num"),
        avg("length").as("length_avg"),
        avg("tags_num").as("tags_num_avg"),
        min("time").as("first_message"),
        max("time").as("last_message")
      )

    dailyUsersActivity
      .write
      .format("jdbc")
      .options(jdbcConnection)
      .mode("overwrite")
      .save()
  }
  else {
    throw new Exception("No data from " + beginDate + " to " + endDate)
  }

}
