# Dockerfile.api
FROM openjdk:11-jre-slim

WORKDIR /app

COPY target/restaurant-topk-api.jar /app/app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# Dockerfile.generator
FROM openjdk:11-jre-slim

WORKDIR /app

COPY target/restaurant-topk-generator.jar /app/app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV ORDERS_PER_SECOND=100

ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# Dockerfile.flink
FROM flink:2.1.0-scala_2.12-java21

COPY target/restaurant-topk-flink.jar /opt/flink/usrlib/restaurant-topk-flink.jar