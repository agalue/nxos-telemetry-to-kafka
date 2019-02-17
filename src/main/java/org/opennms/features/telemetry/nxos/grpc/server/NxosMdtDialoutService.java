package org.opennms.features.telemetry.nxos.grpc.server;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opennms.netmgt.telemetry.ipc.TelemetryProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.StreamObserver;
import com.cisco.nxos.telemetry.MdtDialoutArgs;
import com.cisco.nxos.telemetry.Telemetry;
import com.cisco.nxos.telemetry.gRPCMdtDialoutGrpc;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

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
    private boolean gpbDebug;

    /** The Metrics Registry. */
    private final MetricRegistry metrics;

    /** The Meter for tracking the amount of forwarded messages. */
    private final Meter messagesForwarded;

    /** The Meter for tracking the amount of errors. */
    private final Meter errors;

    /**
     * Instantiates a new NX-OS Telemetry mdt-dialout service.
     *
     * @param metrics the Metrics Registry
     * @param kafkaProducer the Kafka producer
     * @param kafkaTopic the Kafka topic
     * @param minionId the OpenNMS Minion id
     * @param minionLocation the OpenNMS Minion location
     * @param minionAddress the OpenNMS Minion address
     * @param listenerPort the listener port
     * @param gpbDebug the GBP debug flag
     */
    public NxosMdtDialoutService(MetricRegistry metrics, Producer<String, byte[]> kafkaProducer, String kafkaTopic, String minionId, String minionLocation, String minionAddress, int listenerPort, boolean gpbDebug) {
        this.metrics = metrics;
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.minionId = minionId;
        this.minionLocation = minionLocation;
        this.minionAddress = minionAddress;
        this.listenerPort = listenerPort;
        this.gpbDebug = gpbDebug;
        messagesForwarded = metrics.meter("forwarded");
        errors = metrics.meter("errors");
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
                errors.mark();
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
        LOG.info("Sending message to Kafka topic {}", kafkaTopic);
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception == null) {
                LOG.info("Message has been sent to Kafka topic {}", kafkaTopic);
                messagesForwarded.mark();
            } else {
                errors.mark();
                LOG.error("Error writing to topic {} while sending a message of size {}: {}", kafkaTopic, data.size(), exception);
            }
        });
    }

}
