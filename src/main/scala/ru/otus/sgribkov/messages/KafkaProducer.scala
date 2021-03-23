package ru.otus.sgribkov.messages

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.generic.GenericData
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties
import java.time.Instant
import scala.collection.JavaConverters._


object KafkaProducer extends App with ParseJson {

  val topic = "users_messages"
  val kafkaUrl = "http://localhost:29092"
  val schemaRegistryUrl = "http://localhost:8081"
  val intervalSec = 10
  val schema = AvroSchema[InputMessage]
  //--------------------------------------

  val producerProps = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl)
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer")
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
  producerProps.put("schema.registry.url", schemaRegistryUrl)
  val producer = new KafkaProducer[Long, GenericData.Record](producerProps)
  //--------------------------------------

  val httpClient = HttpClients.createDefault
  val httpGet = new HttpGet("https://quotes15.p.rapidapi.com/quotes/random/")
  httpGet.setHeader("x-rapidapi-host", "quotes15.p.rapidapi.com")
  httpGet.setHeader("x-rapidapi-key", "69583006aemshc1e88da72ef736fp123742jsnce80e709be41")
  httpGet.setHeader("useQueryString", "true")
  //--------------------------------------

  try {

    while (1 == 1) {

      val responseTime = Instant.now.getEpochSecond
      val responseEntity = httpClient.execute(httpGet).getEntity
      val json = EntityUtils.toString(responseEntity)

      val message = getCaseClass[InputMessage](json)
      val key = message.id//.toString

      val messageAvro = new GenericData.Record(schema)
      val originator = new GenericData.Record(schema.getField("originator").schema())

      originator.put("id", message.originator.id)
      originator.put("name", message.originator.name)
      originator.put("url", message.originator.url)

      messageAvro.put("id", message.id)
      messageAvro.put("language_code", message.language_code)
      messageAvro.put("content", message.content)
      messageAvro.put("originator", originator)
      messageAvro.put("tags", message.tags.asJavaCollection)
      messageAvro.put("time", responseTime)

      val record = new ProducerRecord(topic, key, messageAvro)
      println(record)
      producer.send(record)

      Thread.sleep(intervalSec * 1000)
    }
  }
  finally {

    httpClient.close()
    producer.close()
  }
  //--------------------------------------

}