/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2018 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2018 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.telemetry.listeners.grpc.nxos;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.netmgt.telemetry.listeners.api.Listener;
import org.opennms.netmgt.telemetry.listeners.api.TelemetryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * The Class NxosGrpcListener.
 * 
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
public class NxosGrpcListener implements Listener {
    private static final Logger LOG = LoggerFactory.getLogger(NxosGrpcListener.class);

    private String name;
    private AsyncDispatcher<TelemetryMessage> dispatcher;
    private Server server;
    private int port;
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setDispatcher(AsyncDispatcher<TelemetryMessage> dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void start() throws InterruptedException {
        LOG.info("Starting gRPC server on port {}", port);
        server = ServerBuilder.forPort(port)
                .addService(new NxosMdtDialoutService(dispatcher, port))
                .build();
        try {
            server.start();
            CompletableFuture.runAsync(() -> {
                try {
                    server.awaitTermination();
                } catch (InterruptedException ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            });
        } catch (IOException e) {
            LOG.error("Cannot initialize gRPC server", e);
        }
    }

    public void stop() throws InterruptedException {
        LOG.info("Shutting down gRPC server on port {}", port);
        server.shutdownNow();
    }
}
