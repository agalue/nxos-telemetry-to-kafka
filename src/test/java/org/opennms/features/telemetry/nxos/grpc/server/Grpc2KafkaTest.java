package org.opennms.features.telemetry.nxos.grpc.server;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.opennms.core.ipc.sink.model.SinkMessageProtos;
import org.opennms.netmgt.telemetry.ipc.TelemetryProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import com.cisco.nxos.telemetry.MdtDialoutArgs;
import com.cisco.nxos.telemetry.gRPCMdtDialoutGrpc;
import com.cisco.nxos.telemetry.Telemetry;
import com.cisco.nxos.telemetry.TelemetryField;

/**
 * The Test Class for Grpc2Kafka.
 *
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
@RunWith(JUnit4.class)
public class Grpc2KafkaTest {

    /** The Constant LOG. */
    private static final Logger LOG = LoggerFactory.getLogger(Grpc2KafkaTest.class);

    /** The gRPC server rule. */
    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

    /**
     * Test gRPC server with Kafka using OpenNMS Minion Metadata.
     *
     * @throws Exception the exception
     */
    @Test
    public void testServer() throws Exception {
        MockProducer<String, byte[]> mockProducer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<MdtDialoutArgs> requestObserver = createObserver(latch, mockProducer, 0, true);

        MdtDialoutArgs requestArgs = MdtDialoutArgs.newBuilder()
                .setReqId(1)
                .setData(ByteString.copyFrom(createBasicTelemetryObject().toByteArray()))
                .build();

        requestObserver.onNext(requestArgs);
        requestObserver.onCompleted();
        latch.await(5, TimeUnit.SECONDS);

        Assert.assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, byte[]> record = mockProducer.history().get(0);

        SinkMessageProtos.SinkMessage sink = SinkMessageProtos.SinkMessage.parseFrom(record.value());
        LOG.debug("Message ID {}", sink.getMessageId());
        LOG.debug("Chunk {}, Total {}", sink.getCurrentChunkNumber(), sink.getTotalChunks());
        TelemetryProtos.TelemetryMessageLog log = TelemetryProtos.TelemetryMessageLog.parseFrom(sink.getContent().toByteArray());
        Telemetry payload = Telemetry.parseFrom(log.getMessage(0).getBytes());
        Assert.assertEquals("agalue", payload.getDataGpbkv(0).getStringValue());
    }

    /**
     * Test gRPC server with Kafka with a big telemetry message
     *
     * @throws Exception the exception
     */
    @Test
    public void testServerWithBigMessage() throws Exception {
        MockProducer<String, byte[]> mockProducer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<MdtDialoutArgs> requestObserver = createObserver(latch, mockProducer, 1000, false);

        MdtDialoutArgs requestArgs = MdtDialoutArgs.newBuilder()
                .setReqId(1)
                .setData(ByteString.copyFrom(createBigTelemetryObject().toByteArray()))
                .build();

        requestObserver.onNext(requestArgs);
        requestObserver.onCompleted();
        latch.await(5, TimeUnit.SECONDS);

        Assert.assertEquals(14, mockProducer.history().size());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (int i = 0; i < mockProducer.history().size(); i++) {
            SinkMessageProtos.SinkMessage sink = SinkMessageProtos.SinkMessage.parseFrom(mockProducer.history().get(i).value());
            LOG.debug("Chunk {} of {}", sink.getCurrentChunkNumber() + 1, sink.getTotalChunks());
            outputStream.write(sink.getContent().toByteArray());
        }

        TelemetryProtos.TelemetryMessageLog log = TelemetryProtos.TelemetryMessageLog.parseFrom(outputStream.toByteArray());
        Telemetry payload = Telemetry.parseFrom(log.getMessage(0).getBytes());
        Assert.assertEquals(1000, payload.getDataGpbkvCount());
    }

    private StreamObserver<MdtDialoutArgs> createObserver(CountDownLatch latch, MockProducer<String, byte[]> mockProducer, int maxBufferSize, boolean debug) {
        final MetricRegistry metrics = new MetricRegistry();
        grpcServerRule.getServiceRegistry().addService(new NxosMdtDialoutService(metrics, mockProducer, "test-topic", "0000", "JUnit", "127.0.0.1", 50001, maxBufferSize,debug));
        gRPCMdtDialoutGrpc.gRPCMdtDialoutStub stub = gRPCMdtDialoutGrpc.newStub(grpcServerRule.getChannel());
        StreamObserver<MdtDialoutArgs> requestObserver = stub.mdtDialout(new StreamObserver<MdtDialoutArgs>() {
            @Override
            public void onNext(MdtDialoutArgs value) {
                LOG.info("Client has sent some data...");
            }

            @Override
            public void onError(Throwable t) {
                LOG.error("An error occurred", t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOG.info("Test Done!");
                latch.countDown();
            }
        });
        return requestObserver;
    }

    private Telemetry createBasicTelemetryObject() {
        TelemetryField field = TelemetryField.newBuilder()
                .setName("owner")
                .setStringValue("agalue")
                .build();

        Telemetry telemetry = Telemetry.newBuilder()
                .setNodeIdStr("nxos")
                .setCollectionEndTime(System.currentTimeMillis())
                .setMsgTimestamp(System.currentTimeMillis())
                .addDataGpbkv(field)
                .build();
        return telemetry;
    }

    private Telemetry createBigTelemetryObject() {
        Telemetry.Builder builder = Telemetry.newBuilder()
                .setNodeIdStr("nxos")
                .setCollectionEndTime(System.currentTimeMillis())
                .setMsgTimestamp(System.currentTimeMillis());
        for (int i = 0; i < 1000; i++) {
            TelemetryField field = TelemetryField.newBuilder()
                    .setName("k" + i)
                    .setStringValue("v" + i)
                    .build();
            builder.addDataGpbkv(field);
        }
        return builder.build();
    }

}
