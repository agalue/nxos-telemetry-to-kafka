package org.opennms.features.telemetry.nxos.grpc.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import mdt_dialout.MdtDialout.MdtDialoutArgs;
import mdt_dialout.gRPCMdtDialoutGrpc;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import telemetry.TelemetryBis.Telemetry;

import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * The Class NxosMdtDialoutService.
 * 
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
public class NxosMdtDialoutService extends gRPCMdtDialoutGrpc.gRPCMdtDialoutImplBase {

    /** The Constant LOG. */
    private static final Logger LOG = LoggerFactory.getLogger(NxosMdtDialoutService.class);

    /** The Kafka producer. */
    private Producer<String, byte[]> kafkaProducer;

    /** The Kafka topic. */
    private String kafkaTopic;

    /** The GPB debug flag. */
    private boolean gpbDebug = false;

    /** The JSON conversion flag. */
    private boolean toJson = false;

    /**
     * Instantiates a new NX-OS Telemetry mdt-dialout service.
     *
     * @param kafkaProducer the Kafka producer
     * @param kafkaTopic the Kafka topic
     * @param gpbDebug the GPB debug flag
     * @param toJson the convert to JSON flag
     */
    public NxosMdtDialoutService(Producer<String, byte[]> kafkaProducer, String kafkaTopic, boolean gpbDebug, boolean toJson) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.gpbDebug = gpbDebug;
        this.toJson = toJson;
    }

    /* (non-Javadoc)
     * @see mdt_dialout.gRPCMdtDialoutGrpc.gRPCMdtDialoutImplBase#mdtDialout(io.grpc.stub.StreamObserver)
     */
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

    /**
     * Log message.
     *
     * @param data the data in bytes
     */
    private void logMessage(ByteString data) {
        if (!gpbDebug) return;
        try {
            Telemetry telemetry = Telemetry.parseFrom(data);
            LOG.info("GPB Message: {}", telemetry);
        } catch (InvalidProtocolBufferException e) {
            LOG.info("Cannot parse payload as GPB. Content: {}", data);
        }
    }

    /**
     * Send message to Kafka.
     *
     * @param data the data in bytes
     */
    private void sendMessageToKafka(ByteString data) {
        byte[] array = null;
        if (toJson) {
            try {
                Telemetry telemetry = Telemetry.parseFrom(data);
                array = JsonFormat.printer().print(telemetry).getBytes();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse payload as GPB. Content: {}", data);
                return;
            }
        } else {
            array = data.toByteArray();
        }
        final ProducerRecord<String, byte[]> record = new ProducerRecord<>(kafkaTopic, array);
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
