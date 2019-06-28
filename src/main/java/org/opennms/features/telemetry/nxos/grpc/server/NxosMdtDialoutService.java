package org.opennms.features.telemetry.nxos.grpc.server;

import java.util.UUID;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.opennms.netmgt.telemetry.ipc.TelemetryProtos;
import org.opennms.core.ipc.sink.model.SinkMessageProtos;

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

    /** The Max Size of the Message Chunk (0 ~ disabled). */
    private Integer maxBufferSize;

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
     * @param maxBufferSize the max message size
     * @param gpbDebug the GBP debug flag
     */
    public NxosMdtDialoutService(MetricRegistry metrics, Producer<String, byte[]> kafkaProducer, String kafkaTopic, String minionId, String minionLocation, String minionAddress, int listenerPort, int maxBufferSize, boolean gpbDebug) {
        this.metrics = metrics;
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.minionId = minionId;
        this.minionLocation = minionLocation;
        this.minionAddress = minionAddress;
        this.listenerPort = listenerPort;
        this.gpbDebug = gpbDebug;
        this.maxBufferSize = maxBufferSize;
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
     * Inspired by:
     * <ul>
     * <li><a href="https://github.com/OpenNMS/opennms/blob/develop/features/telemetry/common/src/main/java/org/opennms/netmgt/telemetry/common/ipc/TelemetrySinkModule.java">TelemetrySinkModule</a></li>
     * <li><a href="https://github.com/OpenNMS/opennms/blob/develop/core/ipc/sink/kafka/client/src/main/java/org/opennms/core/ipc/sink/kafka/client/KafkaRemoteMessageDispatcherFactory.java">KafkaRemoteMessageDispatcherFactory</a></li>
     * </ul>
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

        final String messageId = UUID.randomUUID().toString();
        byte[] sinkMessageContent = log.toByteArray();
        int totalChunks = divide(sinkMessageContent.length, maxBufferSize);

        LOG.info("Sending message of {} bytes, divided into {} chunk", sinkMessageContent.length, totalChunks);
        for (int chunk = 0; chunk < totalChunks; chunk++) {
            final int chunkId = chunk + 1;
            byte[] messageInBytes = wrapMessageToProto(messageId, chunk, totalChunks, sinkMessageContent);
            final ProducerRecord<String, byte[]> record = new ProducerRecord<>(kafkaTopic, messageId, messageInBytes);
            LOG.info("Sending chunk {}/{} to Kafka topic {} using messageId {}", chunkId, totalChunks, kafkaTopic, messageId);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    LOG.info("Message has been sent.");
                    messagesForwarded.mark();
                } else {
                    errors.mark();
                    LOG.error("Error writing to topic {} while sending a message {}/{} (total size {}): {}", kafkaTopic, chunkId, totalChunks, data.size(), exception);
                }
            });
        }
    }

    /**
     * Gets the byte array from the SinkMessage protobuf object.
     *
     * @param messageId the message Id
     * @param chunk the chunk number
     * @param totalChunks the total number of chunks
     * @param sinkMessageContent the byte array of the full original message
     */
    private byte[] wrapMessageToProto(String messageId, int chunk, int totalChunks, byte[] sinkMessageContent) {
        int bufferSize = getRemainingBufferSize(sinkMessageContent.length, chunk);
        ByteString byteString = ByteString.copyFrom(sinkMessageContent, chunk * maxBufferSize, bufferSize);
        SinkMessageProtos.SinkMessage.Builder sinkMessageBuilder = SinkMessageProtos.SinkMessage.newBuilder()
                .setMessageId(messageId)
                .setCurrentChunkNumber(chunk)
                .setTotalChunks(totalChunks)
                .setContent(byteString);
        return sinkMessageBuilder.build().toByteArray();
    }

    /**
     * Gets the remaining buffer size.
     *
     * @param messageSize the message size
     * @param chunk the chunk number
     */
    private int getRemainingBufferSize(int messageSize, int chunk) {
        int bufferSize = messageSize;
        if (maxBufferSize > 0 && messageSize > maxBufferSize) {
            int remaining = messageSize - chunk * maxBufferSize;
            bufferSize = (remaining > maxBufferSize) ? maxBufferSize : remaining;
        }
        return bufferSize;
    }

    /**
     * Returns the result of dividing {@code p} by {@code q} rouded up.
     * Inspired by:
     * <ul>
     * <li><a href="https://github.com/google/guava/blob/master/guava/src/com/google/common/math/IntMath.java#L316">IntMath</a></li>
     * </ul>
     *
     * @param p the dividend
     * @param q the divisor
     */
    private int divide(int p, int q) {
        if (q == 0) {
            return 1;
        }
        int div = p / q;
        int rem = p - q * div; // equal to p % q
        if (rem == 0) {
            return div;
        }
        int signum = 1 | ((p ^ q) >> (Integer.SIZE - 1));
        return div + signum;
    }
}
