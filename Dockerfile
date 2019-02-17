FROM openjdk:8-slim
ENV VERSION 1.0.0-SNAPSHOT
WORKDIR /app
RUN groupadd -r grpc && useradd -r -g grpc grpc
USER grpc
COPY ./target/grpc2kafka-${VERSION}-jar-with-dependencies.jar grpc2kafka.jar
CMD java ${JAVA_OPTS} -jar grpc2kafka.jar -b ${BOOTSTRAP_SERVERS} -l ${MINION_LOCATION} -m ${MINION_ID} -p ${PORT} -t ${TOPIC-OpenNMS.Sink.Telemetry-NXOS}
