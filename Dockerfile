FROM openjdk:17-slim

WORKDIR /app

COPY target/scala-2.12/order-generator_2.12-1.0.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
