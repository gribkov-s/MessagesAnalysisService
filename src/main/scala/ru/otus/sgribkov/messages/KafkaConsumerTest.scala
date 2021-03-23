package ru.otus.sgribkov.messages

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import io.confluent.kafka.schemaregistry.client.rest.RestService

object KafkaConsumerTest extends App {

  val topic = "users_messages"
  val kafkaUrl = "http://localhost:29092"
  val schemaRegistryUrl = "http://localhost:8081"

  //val schemaA = AvroSchema[InputMessage]
  val registry = new RestService(schemaRegistryUrl)
  var schemaAvro = registry.getLatestVersionSchemaOnly(topic + "-value")
  //--------------------------------------

  val spark = SparkSession.builder()
    .appName("KafkaConsumer")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val messageInput = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaUrl)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(from_avro($"value", schemaAvro).as("data"))
    .select("data.*")

  messageInput.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()

}
