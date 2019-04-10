package com.czb1n.learning.kafka.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*
import java.time.Duration


/**
 * Created by czb1n.
 */
fun main() {
    val props = Properties().apply {
        put("bootstrap.servers", "localhost:9092")
        put("group.id", "test")
        put("enable.auto.commit", "true")
        put("auto.commit.interval.ms", "1000")
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    KafkaConsumer<String, String>(props).use {
        it.subscribe(listOf("czb1n-topic"))
        while (true) {
            val messages = it.poll(Duration.ofSeconds(60))
            messages.forEach { m -> println("key = ${m.key()} value = ${m.value()}") }
        }
    }
}