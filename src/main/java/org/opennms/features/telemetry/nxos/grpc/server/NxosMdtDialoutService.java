package org.opennms.features.telemetry.nxos.grpc.server;

import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.StreamObserver;
import mdt_dialout.MdtDialout.MdtDialoutArgs;
import mdt_dialout.gRPCMdtDialoutGrpc;
import telemetry.TelemetryBis.Telemetry;

/**
 * The Class NxosMdtDialoutService.
 * 
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
public class NxosMdtDialoutService extends gRPCMdtDialoutGrpc.gRPCMdtDialoutImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(NxosMdtDialoutService.class);

    private Producer<String, byte[]> kafkaProducer;
    private String kafkaTopic;
    private boolean gpbDebug = false;

    public NxosMdtDialoutService(Producer<String, byte[]> kafkaProducer, String kafkaTopic, boolean gpbDebug) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.gpbDebug = gpbDebug;
    }

    @Override
    public StreamObserver<MdtDialoutArgs> mdtDialout(StreamObserver<MdtDialoutArgs> responseObserver) {
        return new StreamObserver<MdtDialoutArgs>() {
            @Override
            public void onNext(MdtDialoutArgs value) {
                LOG.info("Receiving request ID {} with {} bytes of data", value.getReqId(), value.getData().size());
                logMessage(value.getData());
                sendMessageToKafka(value.getData());
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                LOG.info("Terminating communication...");
                responseObserver.onCompleted();
            }
        };
    }

    private void logMessage(ByteString data) {
        if (!gpbDebug) return;
        try {
            Telemetry telemetry = Telemetry.parseFrom(data);
            LOG.info("GPB Message: {}", telemetry);
        } catch (InvalidProtocolBufferException e) {
            LOG.info("Cannot parse payload as GPB. Content: {}", data);
        }
    }

    private void sendMessageToKafka(ByteString data) {
        final ProducerRecord<String, byte[]> record = new ProducerRecord<>(kafkaTopic, data.toByteArray());
        try {
            LOG.info("Sending message to Kafka topic {}", kafkaTopic);
            final Future<RecordMetadata> future = kafkaProducer.send(record);
            future.get();
            LOG.info("Message has been sent to Kafka topic {}", kafkaTopic);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while sending message to topic {}.", kafkaTopic, e);
        } catch (ExecutionException e) {
            LOG.error("Error occurred while sending message to topic {}.", kafkaTopic, e);
        }
    }
}
