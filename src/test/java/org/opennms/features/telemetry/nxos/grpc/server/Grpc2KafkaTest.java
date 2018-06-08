package org.opennms.features.telemetry.nxos.grpc.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import mdt_dialout.MdtDialout.MdtDialoutArgs;
import mdt_dialout.gRPCMdtDialoutGrpc;
import telemetry.TelemetryBis.Telemetry;
import telemetry.TelemetryBis.TelemetryField;

/**
 * The Test Class for Grpc2Kafka
 * 
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
@RunWith(JUnit4.class)
public class Grpc2KafkaTest {
    private static final Logger LOG = LoggerFactory.getLogger(Grpc2KafkaTest.class);

    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

    @Test
    public void testServer() throws Exception {
        MockProducer<String, byte[]> mockProducer = new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
        grpcServerRule.getServiceRegistry().addService(new NxosMdtDialoutService(mockProducer, "test-topic", true));
        gRPCMdtDialoutGrpc.gRPCMdtDialoutStub stub = gRPCMdtDialoutGrpc.newStub(grpcServerRule.getChannel());

        CountDownLatch latch = new CountDownLatch(1);

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

        TelemetryField field = TelemetryField.newBuilder().setName("owner").setStringValue("agalue").build();
        Telemetry telemetry = Telemetry.newBuilder()
                .setNodeIdStr("nxos")
                .setCollectionEndTime(System.currentTimeMillis())
                .setMsgTimestamp(System.currentTimeMillis())
                .addDataGpbkv(field)
                .build();

        MdtDialoutArgs requestArgs = MdtDialoutArgs.newBuilder()
                .setReqId(1)
                .setData(ByteString.copyFrom(telemetry.toByteArray()))
                .build();
        requestObserver.onNext(requestArgs);
        requestObserver.onCompleted();

        latch.await(5, TimeUnit.SECONDS);

        Assert.assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, byte[]> record = mockProducer.history().get(0);
        Telemetry kafkaPayload = Telemetry.parseFrom(record.value());
        Assert.assertEquals("agalue", kafkaPayload.getDataGpbkv(0).getStringValue());
    }

}
