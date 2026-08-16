# --- Build stage ---
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.5_8_1.9.0_2.12.18 AS build
WORKDIR /app
COPY project project
COPY build.sbt .
RUN sbt update
COPY src src
RUN sbt package

# --- Runtime stage ---
FROM apache/spark:3.5.1-scala
WORKDIR /app
COPY --from=build /app/target/scala-2.12/batch-job_2.12-1.0.jar app.jar

ENV JDBC_URL=jdbc:postgresql://postgres:5432/retail
ENV JDBC_USER=postgres
ENV JDBC_PASSWORD=postgres

# No --date is baked in here on purpose — this image is meant to be run
# on-demand (`docker run ... --date 2026-08-15`) or on a schedule (a k8s
# CronJob later), and the date should be supplied at run time, not baked
# into the image. See the same --conf spark.jars.ivy note as the streaming
# job's Dockerfile for why that flag is there.
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--master", "local[*]", \
  "--class", "com.retailpipeline.DailyReconciliation", \
  "--conf", "spark.jars.ivy=/tmp/.ivy2", \
  "--packages", "org.postgresql:postgresql:42.7.3", \
  "/app/app.jar"]
