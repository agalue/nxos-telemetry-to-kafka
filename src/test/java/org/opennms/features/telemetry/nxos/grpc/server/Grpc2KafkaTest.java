package org.opennms.features.telemetry.nxos.grpc.server;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import mdt_dialout.MdtDialout.MdtDialoutArgs;
import mdt_dialout.gRPCMdtDialoutGrpc;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import telemetry.TelemetryBis.Telemetry;
import telemetry.TelemetryBis.TelemetryField;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
     * Test the gRPC server with Kafka.
     *
     * @throws Exception the exception
     */
    @Test
    public void testServer() throws Exception {
        MockProducer<String, byte[]> mockProducer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        grpcServerRule.getServiceRegistry().addService(new NxosMdtDialoutService(mockProducer, "test-topic", true, false));
        gRPCMdtDialoutGrpc.gRPCMdtDialoutStub stub = gRPCMdtDialoutGrpc.newStub(grpcServerRule.getChannel());

        final CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<MdtDialoutArgs> requestObserver = buildRequestObserver(stub, latch);
        requestObserver.onNext(buildTestDialoutArgs());
        requestObserver.onCompleted();
        latch.await(5, TimeUnit.SECONDS);

        Assert.assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, byte[]> record = mockProducer.history().get(0);
        Telemetry kafkaPayload = Telemetry.parseFrom(record.value());
        Assert.assertEquals("agalue", kafkaPayload.getDataGpbkv(0).getStringValue());
    }

    /**
     * Test the gRPC server with Kafka using JSON payload.
     *
     * @throws Exception the exception
     */
    @Test
    public void testServerWithJson() throws Exception {
        MockProducer<String, byte[]> mockProducer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        grpcServerRule.getServiceRegistry().addService(new NxosMdtDialoutService(mockProducer, "test-topic", true, true));
        gRPCMdtDialoutGrpc.gRPCMdtDialoutStub stub = gRPCMdtDialoutGrpc.newStub(grpcServerRule.getChannel());

        final CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<MdtDialoutArgs> requestObserver = buildRequestObserver(stub, latch);
        requestObserver.onNext(buildTestDialoutArgs());
        requestObserver.onCompleted();
        latch.await(5, TimeUnit.SECONDS);

        Assert.assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, byte[]> record = mockProducer.history().get(0);
        String jsonStr = new String(record.value());
        LOG.info("Converted JSON: {}", jsonStr);
        JSONObject json = new JSONObject(jsonStr);
        Assert.assertEquals("nxos", json.get("nodeIdStr"));
        Assert.assertEquals("agalue", json.getJSONArray("dataGpbkv").getJSONObject(0).getString("stringValue"));
    }

    /**
     * Builds the request observer.
     *
     * @param stub the gRPC Stub
     * @param latch the count down latch object
     * @return the stream observer object
     */
    private StreamObserver<MdtDialoutArgs> buildRequestObserver(final gRPCMdtDialoutGrpc.gRPCMdtDialoutStub stub, final CountDownLatch latch) {
        return stub.mdtDialout(new StreamObserver<MdtDialoutArgs>() {
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

    }
    
    /**
     * Builds a test MdtDialoutArgs object.
     *
     * @return the new MdtDialoutArgs object
     */
    private MdtDialoutArgs buildTestDialoutArgs() {
        TelemetryField field = TelemetryField.newBuilder().setName("owner").setStringValue("agalue").build();
        Telemetry telemetry = Telemetry.newBuilder()
                .setNodeIdStr("nxos")
                .setCollectionEndTime(System.currentTimeMillis())
                .setMsgTimestamp(System.currentTimeMillis())
                .addDataGpbkv(field)
                .build();

        return MdtDialoutArgs.newBuilder()
                .setReqId(1)
                .setData(ByteString.copyFrom(telemetry.toByteArray()))
                .build();
    }
}
