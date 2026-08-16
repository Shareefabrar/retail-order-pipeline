package com.retailpipeline

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import java.sql.{Connection, DriverManager, PreparedStatement}

/** Reads order events from Kafka, aggregates revenue/order-count into
  * 1-minute tumbling windows, and upserts the results into Postgres'
  * `live_metrics` table. Malformed events are routed to a separate
  * dead-letter stream (printed to console here; swap for a real sink,
  * e.g. another Kafka topic or an S3 path, in a production version).
  *
  * Design note on why there's only ONE `groupBy(window(...))` here: an
  * earlier version of this job did `groupBy(window, sku)` to get per-SKU
  * revenue, then a second `groupBy(window)` on top of that to roll it up
  * into a per-window total + top SKU. Spark's Structured Streaming engine
  * rejects that at query-start time (AnalysisException: "possible
  * 'correctness' issue due to global watermark") because chaining two
  * stateful aggregations against the same watermark can silently drop rows
  * the first aggregation emits as "late" by the time the second one sees
  * them. That's a real risk, not a false positive — so instead, this
  * version does exactly one stateful aggregation (collecting per-SKU
  * revenue into a list per window), and figures out the top SKU afterward
  * as a plain, non-streaming DataFrame computation inside `foreachBatch`,
  * which operates on a static micro-batch and isn't subject to that check.
  */
object LiveOrderAggregator {

  private val orderSchema: StructType = StructType(Seq(
    StructField("order_id",    StringType,  nullable = true),
    StructField("customer_id", StringType,  nullable = true),
    StructField("sku",         StringType,  nullable = true),
    StructField("quantity",    IntegerType, nullable = true),
    StructField("price",       DoubleType,  nullable = true),
    StructField("timestamp",   LongType,    nullable = true)  // epoch millis
  ))

  // For local dev these default to the host-facing localhost ports (for
  // running via bare `spark-submit`). When containerized and joined to the
  // same Docker network as Kafka/Postgres, override with -e flags using
  // the containers' internal service names, e.g.:
  //   -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092
  //   -e JDBC_URL=jdbc:postgresql://postgres:5432/retail
  private val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
  private val jdbcUrl      = sys.env.getOrElse("JDBC_URL", "jdbc:postgresql://localhost:5432/retail")
  private val jdbcUser     = sys.env.getOrElse("JDBC_USER", "postgres")
  private val jdbcPassword = sys.env.getOrElse("JDBC_PASSWORD", "postgres")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LiveOrderAggregator")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaRaw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", "orders")
      .option("startingOffsets", "latest")
      .load()

    val rawStrings = kafkaRaw.selectExpr("CAST(value AS STRING) AS json_str")

    val parsed = rawStrings
      .withColumn("parsed", from_json(col("json_str"), orderSchema))
      .select(col("json_str"), col("parsed.*"))

    // Records that failed to parse against orderSchema come back with all
    // fields null — that's our dead-letter signal.
    val validOrders = parsed
      .filter(col("order_id").isNotNull)
      // to_timestamp() on a numeric column interprets it as seconds since
      // the epoch, so divide the millis down first.
      .withColumn("event_time", to_timestamp(col("timestamp").cast(DoubleType) / 1000.0))
      .withColumn("revenue", col("quantity") * col("price"))

    val deadLetters = parsed.filter(col("order_id").isNull)

    // The ONE stateful aggregation: total revenue, order count, and a raw
    // list of (sku, revenue) pairs per window. Figuring out the top SKU
    // from that list happens later, outside the streaming engine.
    val perWindowAgg = validOrders
      .withWatermark("event_time", "30 seconds")
      .groupBy(window(col("event_time"), "1 minute"))
      .agg(
        sum("revenue").as("total_revenue"),
        count("*").as("order_count"),
        collect_list(struct(col("sku"), col("revenue"))).as("sku_revenues")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("total_revenue"),
        col("order_count"),
        col("sku_revenues")
      )

    // Runs once per micro-batch on a plain, static DataFrame — safe to do
    // as much multi-stage aggregation here as you want, since none of it
    // is part of the incremental streaming execution plan.
    def upsertToPostgres(batchDf: DataFrame, batchId: Long): Unit = {
      if (batchDf.isEmpty) return

      val exploded = batchDf
        .withColumn("sku_rev", explode(col("sku_revenues")))
        .select(
          col("window_start"), col("window_end"),
          col("sku_rev.sku").as("sku"), col("sku_rev.revenue").as("revenue")
        )

      val skuTotals = exploded
        .groupBy("window_start", "window_end", "sku")
        .agg(sum("revenue").as("sku_total_revenue"))

      val rankSpec = Window.partitionBy("window_start", "window_end")
        .orderBy(col("sku_total_revenue").desc)

      val topSkuPerWindow = skuTotals
        .withColumn("rank", row_number().over(rankSpec))
        .filter(col("rank") === 1)
        .select(col("window_start"), col("window_end"), col("sku").as("top_sku"))

      val finalDf = batchDf.drop("sku_revenues")
        .join(topSkuPerWindow, Seq("window_start", "window_end"), "left")

      val conn: Connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      try {
        val upsertSql =
          """
            |INSERT INTO live_metrics (window_start, window_end, total_revenue, order_count, top_sku)
            |VALUES (?, ?, ?, ?, ?)
            |ON CONFLICT (window_start, window_end)
            |DO UPDATE SET total_revenue = EXCLUDED.total_revenue,
            |              order_count   = EXCLUDED.order_count,
            |              top_sku       = EXCLUDED.top_sku
          """.stripMargin
        val stmt: PreparedStatement = conn.prepareStatement(upsertSql)

        var rowCount = 0
        finalDf.collect().foreach { row: Row =>
          stmt.setTimestamp(1, row.getAs[java.sql.Timestamp]("window_start"))
          stmt.setTimestamp(2, row.getAs[java.sql.Timestamp]("window_end"))
          stmt.setDouble(3, row.getAs[Double]("total_revenue"))
          stmt.setInt(4, row.getAs[Long]("order_count").toInt)
          stmt.setString(5, row.getAs[String]("top_sku"))
          stmt.addBatch()
          rowCount += 1
        }
        stmt.executeBatch()
        println(s"[batch $batchId] upserted $rowCount window(s) into live_metrics")
      } finally {
        conn.close()
      }
    }

    val metricsQuery = perWindowAgg.writeStream
      .outputMode("update")
      .foreachBatch(upsertToPostgres _)
      .option("checkpointLocation", "/tmp/spark-checkpoints/live-order-aggregator")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    deadLetters.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    // Waits on whichever of the two active queries stops or errors first,
    // so a silent failure in either stream surfaces immediately instead of
    // being masked by sequential awaitTermination() calls.
    spark.streams.awaitAnyTermination()
  }
}
