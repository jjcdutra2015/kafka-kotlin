package com.git.jjcdutra2015.ecommerce

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import java.util.concurrent.ExecutionException

@Throws(ExecutionException::class, InterruptedException::class)
fun main() {
    val producer = KafkaProducer<String, String>(properties())
    val value = "12313, 64655, 87987987987"
    val record = ProducerRecord("ECOMMERCE_NEW_ORDER", value, value)
    producer.send(record) { data: RecordMetadata, ex: Exception? ->
        if (ex != null) {
            ex.printStackTrace()
            return@send
        }
        println("Sucesso enviando: ${data.topic()} ::: Partition ${data.partition()} / Tópico ${data.topic()} / Offset ${data.offset()} / Timestamp ${data.timestamp()}")
    }.get()
}

private fun properties(): Properties {
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    return properties
}