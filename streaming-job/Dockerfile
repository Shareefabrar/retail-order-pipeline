# --- Build stage: compile with sbt (produces a thin jar — Spark and the
# Kafka/Postgres connectors are supplied at runtime, not bundled) ---
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.5_8_1.9.0_2.12.18 AS build
WORKDIR /app
COPY project project
COPY build.sbt .
RUN sbt update
COPY src src
RUN sbt package

# --- Runtime stage: official Apache Spark image, so spark-submit and the
# core Spark jars are already present ---
FROM apache/spark:3.5.1-scala
WORKDIR /app
COPY --from=build /app/target/scala-2.12/streaming-job_2.12-1.0.jar app.jar

# Overridable at `docker run -e ...` / in the k8s manifest later. Defaults
# assume this container joins the same Docker network as the kafka/postgres
# containers (see the docker-compose profile in the README) and reaches
# them by service name.
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV JDBC_URL=jdbc:postgresql://postgres:5432/retail
ENV JDBC_USER=postgres
ENV JDBC_PASSWORD=postgres

# --packages pulls the Kafka connector + Postgres driver from Maven Central
# the first time the container starts, so it needs outbound network access
# on first run (they land under /tmp/.ivy2 inside the container, which is
# NOT persisted across container restarts unless you mount a volume there —
# fine for a demo, but worth a volume mount if you're iterating a lot and
# want to avoid re-downloading every time).
# spark.jars.ivy is pinned to /tmp explicitly since the official Spark
# image runs as a non-root user that may not have write access to the
# default ivy cache location.
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--master", "local[*]", \
  "--class", "com.retailpipeline.LiveOrderAggregator", \
  "--conf", "spark.jars.ivy=/tmp/.ivy2", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
  "/app/app.jar"]
