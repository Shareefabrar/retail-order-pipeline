package com.retailpipeline

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import java.time.Instant
import java.util.UUID
import scala.util.Random

/** Publishes synthetic e-commerce order events to the Kafka topic "orders".
  *
  * Includes deliberate spikes (for anomaly detection downstream) and a small
  * rate of malformed records (to exercise dead-letter handling in the
  * streaming job). Connects from the host machine, so it uses the
  * PLAINTEXT_HOST listener on localhost:9093.
  */
object OrderProducer {

  private val skus = Array(
    "SKU-001", "SKU-002", "SKU-003", "SKU-004", "SKU-005", "SKU-006", "SKU-007"
  )
  private val random = new Random()

  private def randomOrderJson(): String = {
    val orderId = UUID.randomUUID().toString
    val customerId = s"CUST-${random.nextInt(500)}"

    // Weight SKU-001 heavily so "top SKU" aggregation is meaningful to look at.
    val sku = if (random.nextDouble() < 0.4) skus(0) else skus(random.nextInt(skus.length))

    val baseQuantity = 1 + random.nextInt(5)
    // ~1% of orders are a big spike, to give the batch job's anomaly
    // detector something real to flag.
    val quantity = if (random.nextDouble() < 0.01) baseQuantity * 20 else baseQuantity

    val price = BigDecimal(5 + random.nextDouble() * 195)
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
    // Epoch millis, not an ISO string: Spark's to_timestamp() parsing of
    // Instant.toString's nanosecond-precision "...Z" format is unreliable
    // and can silently null out every event_time. A plain number sidesteps
    // that entirely and is the more common convention in real pipelines.
    val timestampMillis = Instant.now().toEpochMilli

    // ~2% of records are intentionally malformed, to exercise the
    // streaming job's dead-letter path.
    if (random.nextDouble() < 0.02) {
      s"""{"order_id":"$orderId","customer_id":"$customerId","sku":"$sku",BROKEN_JSON"""
    } else {
      s"""{"order_id":"$orderId","customer_id":"$customerId","sku":"$sku","quantity":$quantity,"price":$price,"timestamp":$timestampMillis}"""
    }
  }

  def main(args: Array[String]): Unit = {
    // Defaults to localhost:9093 (the host-facing listener) for running
    // straight on your machine via `sbt run`. When containerized and
    // joined to the same Docker network as the Kafka container, override
    // with -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 (the internal listener).
    val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // Small tuning so records go out promptly for a low-volume local demo.
    props.put("linger.ms", "10")

    val producer = new KafkaProducer[String, String](props)
    val topic = "orders"

    println(s"Order producer starting -> topic '$topic' @ $bootstrapServers (Ctrl+C to stop)")

    sys.addShutdownHook {
      println("Shutting down producer...")
      producer.flush()
      producer.close()
    }

    var sent = 0L
    while (true) {
      val json = randomOrderJson()
      val record = new ProducerRecord[String, String](topic, json)
      producer.send(record, (_, exception) => {
        if (exception != null) println(s"Send failed: ${exception.getMessage}")
      })
      sent += 1
      if (sent % 25 == 0) println(s"Sent $sent orders so far... last: $json")
      Thread.sleep(200)
    }
  }
}
