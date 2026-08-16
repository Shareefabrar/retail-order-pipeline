name := "streaming-job"
version := "1.0"
// Spark 3.5.x is built against Scala 2.12 — match it, even though the
// order-generator above uses 2.13. This is normal in Spark shops: the app
// code targets whatever Scala version the Spark build requires.
scalaVersion := "2.12.18"

// All "provided" because these are supplied at runtime via
// `spark-submit --packages ...` (see the run command in the README).
// This keeps the built jar small and avoids version conflicts with the
// cluster's own Spark/Kafka/Postgres-driver jars.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql"            % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1" % "provided",
  "org.postgresql"    % "postgresql"           % "42.7.3" % "provided"
)
