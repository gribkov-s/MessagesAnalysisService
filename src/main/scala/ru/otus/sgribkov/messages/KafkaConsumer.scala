package ru.otus.sgribkov.messages

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.SchemaConverters

object KafkaConsumer extends App {

  val topic = "users_messages"
  val kafkaUrl = "http://localhost:29092"
  val schemaRegistryUrl = "http://localhost:8081"
  val hdfsDestinationFolder = "hdfs://localhost:9000/data/messages/details"
  val hdfsCheckpointLocation = "hdfs://localhost:9000/data/checkpoints"
  val windowInterval = "1 minute"
  val jdbcConnection = Map(
    "url" -> "jdbc:postgresql://localhost:5432/postgres",
    "dbtable" -> "public.tags_by_time_intervals",
    "user" -> "docker",
    "password" -> "docker",
    "driver" -> "org.postgresql.Driver"
  )


  val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
  val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)
  val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema
  var sparkSchema = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))
  //--------------------------------------

  val spark = SparkSession.builder()
    .appName("KafkaConsumer")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  spark.udf.register("deserialize", (bytes: Array[Byte]) =>
    kafkaAvroDeserializer.deserialize(bytes)
  )
  //--------------------------------------

  val messageInput = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaUrl)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("""deserialize(value) AS message""")
    .select(from_json($"message", sparkSchema.dataType).alias("data"))
    .selectExpr(
      "data.id",
      "data.language_code",
      "data.content",
      "data.originator.name AS originator",
      "data.tags",
      "to_timestamp(from_unixtime(data.time)) AS time",
      "floor(data.time / 60) * 60 AS _time_key"
    )
  //--------------------------------------

    def writeToHDFSandDB(data: DataFrame, batchId: Long): Unit = {

      data.persist()

      data.write
        .format("parquet")
        .mode("append")
        .partitionBy("_time_key")
        .option("checkpointLocation", hdfsCheckpointLocation)
        .save(hdfsDestinationFolder)

      val tagsCounter = data
        .selectExpr("time", "explode(tags) AS tag")
        .withWatermark("time", windowInterval)
        .groupBy(
          window($"time", windowInterval, windowInterval), $"tag")
        .count()
        .selectExpr(
          "window.start AS begin_time",
          "window.end AS end_time",
          "tag",
          "count"
          )

      tagsCounter
        .write
        .format("jdbc")
        .options(jdbcConnection)
        .mode("append")
        .save()

      data.unpersist()
  }

  messageInput
    .writeStream
    .foreachBatch(writeToHDFSandDB _)
    .start()
    .awaitTermination()

}
