# --- Build stage: compile + assemble a fat jar with sbt ---
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.5_8_1.9.0_2.12.18 AS build
WORKDIR /app

# Copy dependency-defining files first so Docker can cache the
# dependency-resolution layer separately from source changes.
COPY project project
COPY build.sbt .
RUN sbt update

COPY src src
RUN sbt assembly

# --- Runtime stage: just a JRE + the fat jar, nothing else ---
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/target/scala-2.13/order-generator.jar app.jar

# Overridable at `docker run -e ...` time. Defaults assume standalone
# `docker run` against a Kafka container reachable as "kafka" on a shared
# Docker network (see the docker-compose profile in the README) — adjust
# if you're running this some other way.
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

ENTRYPOINT ["java", "-jar", "app.jar"]
