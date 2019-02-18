FROM openjdk:11-jre-slim
ENV VERSION 1.0.0-SNAPSHOT
LABEL maintainer="Alejandro Galue <agalue@opennms.org>" name="NX-OS gRPC to Kafka" version="${VERSION}"
WORKDIR /app
RUN groupadd -r grpc && useradd -r -g grpc grpc
USER grpc
COPY ./target/grpc2kafka-${VERSION}-jar-with-dependencies.jar grpc2kafka.jar
COPY ./docker-entrypoint.sh .
ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
