package com.retailpipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager}
import java.time.LocalDate

/** Nightly batch reconciliation job.
  *
  * Re-reads everything the streaming job wrote to `live_metrics` for a given
  * day, recomputes the day's totals as the source of truth (streaming
  * aggregates can double-count or drift slightly on late data / restarts,
  * so a batch reconciliation pass is standard practice), and runs a simple
  * statistical anomaly check on hourly order volume.
  *
  * Note on the anomaly detection: this uses a plain z-score check
  * (Spark SQL aggregations — mean/stddev), not Spark MLlib, because at this
  * data volume a full MLlib pipeline (e.g. a fitted GaussianMixture or
  * IsolationForest-style model) would be overkill and harder to explain.
  * If you want to demonstrate MLlib specifically, swap the anomaly section
  * below for a `org.apache.spark.ml.feature.VectorAssembler` +
  * `org.apache.spark.ml.stat.Summarizer` pipeline, or a
  * `org.apache.spark.ml.clustering.KMeans` outlier-distance approach —
  * worth calling out as a "next step" in your README either way.
  */
object DailyReconciliation {

  // Same env-var override pattern as the streaming job — see comments
  // there. Defaults assume you're running bare-metal against the
  // docker-compose Postgres on localhost.
  private val jdbcUrl      = sys.env.getOrElse("JDBC_URL", "jdbc:postgresql://localhost:5432/retail")
  private val jdbcUser     = sys.env.getOrElse("JDBC_USER", "postgres")
  private val jdbcPassword = sys.env.getOrElse("JDBC_PASSWORD", "postgres")

  def main(args: Array[String]): Unit = {
    val targetDate = args.sliding(2).collectFirst {
      case Array("--date", d) => d
    }.getOrElse(LocalDate.now().toString)

    val spark = SparkSession.builder()
      .appName("DailyReconciliation")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val liveMetrics = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "live_metrics")
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .load()

    val dayData = liveMetrics.filter(to_date(col("window_start")) === lit(targetDate))

    if (dayData.isEmpty) {
      println(s"No live_metrics rows found for $targetDate — nothing to reconcile.")
      spark.stop()
      return
    }

    // --- Daily summary ---
    val summaryRow = dayData
      .agg(sum("order_count").as("total_orders"), sum("total_revenue").as("total_revenue"))
      .first()

    val totalOrders  = summaryRow.getAs[Long]("total_orders")
    val totalRevenue = summaryRow.getAs[Double]("total_revenue")
    val avgOrderValue = if (totalOrders > 0) totalRevenue / totalOrders else 0.0

    writeDailySummary(targetDate, totalOrders, totalRevenue, avgOrderValue)
    println(f"Reconciled $targetDate: $totalOrders%d orders, revenue=$totalRevenue%.2f, avgOrderValue=$avgOrderValue%.2f")

    // --- Hourly anomaly detection (z-score on order volume) ---
    val hourly = dayData
      .withColumn("hour", hour(col("window_start")))
      .groupBy("hour")
      .agg(sum("order_count").as("hourly_orders"))

    val stats = hourly
      .agg(avg("hourly_orders").as("mean"), stddev_pop("hourly_orders").as("stddev"))
      .first()

    val mean   = stats.getAs[Double]("mean")
    val stddev = Option(stats.getAs[java.lang.Double]("stddev")).map(_.doubleValue()).getOrElse(0.0)
    val zThreshold = 2.0

    if (stddev > 0) {
      val expectedLow  = mean - zThreshold * stddev
      val expectedHigh = mean + zThreshold * stddev

      val anomalies = hourly.filter(abs(col("hourly_orders") - lit(mean)) > lit(zThreshold * stddev))
      val flaggedCount = anomalies.count()

      if (flaggedCount > 0) {
        anomalies.collect().foreach { row =>
          val hourlyOrders = row.getAs[Long]("hourly_orders")
          writeAnomaly(targetDate, "hourly_order_count", hourlyOrders.toDouble, expectedLow, expectedHigh)
        }
        println(s"Flagged $flaggedCount anomalous hour(s) for $targetDate (expected range " +
          f"$expectedLow%.1f - $expectedHigh%.1f orders/hour)")
      } else {
        println(s"No anomalies detected for $targetDate.")
      }
    } else {
      println(s"Not enough variance in hourly data for $targetDate to run anomaly detection.")
    }

    spark.stop()
  }

  private def writeDailySummary(date: String, totalOrders: Long, totalRevenue: Double, avgOrderValue: Double): Unit = {
    val conn: Connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    try {
      val sql =
        """
          |INSERT INTO daily_summary (summary_date, total_orders, total_revenue, avg_order_value)
          |VALUES (?, ?, ?, ?)
          |ON CONFLICT (summary_date)
          |DO UPDATE SET total_orders    = EXCLUDED.total_orders,
          |              total_revenue   = EXCLUDED.total_revenue,
          |              avg_order_value = EXCLUDED.avg_order_value
        """.stripMargin
      val stmt = conn.prepareStatement(sql)
      stmt.setDate(1, java.sql.Date.valueOf(date))
      stmt.setInt(2, totalOrders.toInt)
      stmt.setDouble(3, totalRevenue)
      stmt.setDouble(4, avgOrderValue)
      stmt.executeUpdate()
    } finally {
      conn.close()
    }
  }

  private def writeAnomaly(date: String, metric: String, value: Double, low: Double, high: Double): Unit = {
    val conn: Connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    try {
      val sql =
        """
          |INSERT INTO anomalies (detected_date, metric, value, expected_low, expected_high)
          |VALUES (?, ?, ?, ?, ?)
        """.stripMargin
      val stmt = conn.prepareStatement(sql)
      stmt.setDate(1, java.sql.Date.valueOf(date))
      stmt.setString(2, metric)
      stmt.setDouble(3, value)
      stmt.setDouble(4, low)
      stmt.setDouble(5, high)
      stmt.executeUpdate()
    } finally {
      conn.close()
    }
  }
}
