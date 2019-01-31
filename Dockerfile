FROM maven:3-jdk-8 as build
WORKDIR /app
COPY . .
RUN mvn -DskipTests clean install

FROM openjdk:8-slim
ENV VERSION 1.0.0-SNAPSHOT
WORKDIR /app
COPY --from=build /app/target/grpc2kafka-${VERSION}-jar-with-dependencies.jar grpc2kafka.jar
CMD java ${JAVA_OPTS} -jar grpc2kafka.jar -b ${BOOTSTRAP_SERVERS} -l ${MINION_LOCATION} -m ${MINION_ID} -p ${PORT} -t ${TOPIC-OpenNMS.Sink.Telemetry-NXOS}
