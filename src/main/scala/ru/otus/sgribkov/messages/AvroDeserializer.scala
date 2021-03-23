package ru.otus.sgribkov.messages

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord

class AvroDeserializer extends AbstractKafkaAvroDeserializer {
  def this(client: SchemaRegistryClient) {
    this()
    this.schemaRegistry = client
  }

  override def deserialize(bytes: Array[Byte]): String = {
    val genericRecord = super.deserialize(bytes).asInstanceOf[GenericRecord]
    genericRecord.toString
  }
}
