package com.czb1n.learning.kafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Instant
import java.util.*

/**
 * Created by czb1n.
 */
fun main() {
    val props = Properties().apply {
        put("bootstrap.servers", "localhost:9092")
        put("acks", "all")
        put("retries", 0)
        put("batch.size", 16384)
        put("linger.ms", 1)
        put("buffer.memory", 33554432)
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    }

    KafkaProducer<String, String>(props).use {
        while (true) {
            val message = ProducerRecord("czb1n-topic", "current-time", Instant.now().toString())
            it.send(message)
            Thread.sleep(1000)
        }
    }
}