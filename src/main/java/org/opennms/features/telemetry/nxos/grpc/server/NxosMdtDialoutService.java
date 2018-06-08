package org.opennms.features.telemetry.nxos.grpc.server;

import java.nio.file.StandardOpenOption;

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

    private boolean gpbDebug = false;

    public NxosMdtDialoutService(boolean gpbDebug) {
        this.gpbDebug = gpbDebug;
    }

    @Override
    public StreamObserver<MdtDialoutArgs> mdtDialout(StreamObserver<MdtDialoutArgs> responseObserver) {
        return new StreamObserver<MdtDialoutArgs>() {
            @Override
            public void onNext(MdtDialoutArgs value) {
                LOG.info("Receiving request ID {} with {} bytes of data", value.getReqId(), value.getData().size());
                logMessage(value.getData());
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

}
