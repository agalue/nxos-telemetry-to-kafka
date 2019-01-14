package org.opennms.features.telemetry.nxos.grpc.server;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.opennms.netmgt.telemetry.ipc.TelemetryProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.StreamObserver;
import com.cisco.nxos.telemetry.MdtDialoutArgs;
import com.cisco.nxos.telemetry.Telemetry;
import com.cisco.nxos.telemetry.gRPCMdtDialoutGrpc;

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

    /** The OpenNMS Minion id. */
    private String minionId;

    /** The OpenNMS Minion location. */
    private String minionLocation;

    /** The OpenNMS Minion address. */
    private String minionAddress;

    /** The listener port. */
    private int listenerPort;

    /** The GPB debug flag. */
    private boolean gpbDebug = false;

    /**
     * Instantiates a new NX-OS Telemetry mdt-dialout service.
     *
     * @param kafkaProducer the Kafka producer
     * @param kafkaTopic the Kafka topic
     * @param minionId the OpenNMS Minion id
     * @param minionLocation the OpenNMS Minion location
     * @param minionAddress the OpenNMS Minion address
     * @param listenerPort the listener port
     * @param gpbDebug the GBP debug flag
     */
    public NxosMdtDialoutService(Producer<String, byte[]> kafkaProducer, String kafkaTopic, String minionId, String minionLocation, String minionAddress, int listenerPort, boolean gpbDebug) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.minionId = minionId;
        this.minionLocation = minionLocation;
        this.minionAddress = minionAddress;
        this.listenerPort = listenerPort;
        this.gpbDebug = gpbDebug;
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
        TelemetryProtos.TelemetryMessage message = TelemetryProtos.TelemetryMessage.newBuilder()
                .setBytes(data)
                .setTimestamp(System.currentTimeMillis())
                .build();
        TelemetryProtos.TelemetryMessageLog log = TelemetryProtos.TelemetryMessageLog.newBuilder()
                .setSystemId(minionId)
                .setLocation(minionLocation)
                .setSourceAddress(minionAddress)
                .setSourcePort(listenerPort)
                .addMessage(message)
                .build();
        final ProducerRecord<String, byte[]> record = new ProducerRecord<>(kafkaTopic, log.toByteArray());
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
