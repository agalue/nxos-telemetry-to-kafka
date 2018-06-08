package org.opennms.features.telemetry.nxos.grpc.server;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * The Class Grpc2Kafka.
 * 
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
public class Grpc2Kafka {

    private static final Logger LOG = LoggerFactory.getLogger(Grpc2Kafka.class);

    public static final int DEFAULT_GRPC_PORT = 50051;

    public static void main(String[] args) throws IOException, InterruptedException {
        final Options options = new Options()
                .addOption("p", "port", true, "gRPC server listener port.\nDefault: " + DEFAULT_GRPC_PORT)
                .addOption("d", "debug", false, "Show message on logs.")
                .addOption("h", "help", false, "Show this help.");

        final CommandLineParser cmdLineParser = new DefaultParser();
        CommandLine cli = null;
        try {
            cli = cmdLineParser.parse(options, args);
        } catch (ParseException parseException) {
            System.err.printf("ERROR: Unable to parse command-line arguments due to: %s\n", parseException);
            printHelp(options);
            System.exit(0);
        }

        if (cli.hasOption('h')) {
            printHelp(options);
            System.exit(1);
        }

        Integer serverPort = cli.hasOption('p') ? new Integer(cli.getOptionValue('p')) : DEFAULT_GRPC_PORT;
        Server server = buildServer(serverPort, cli.hasOption('d'));
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown request...");
            server.shutdown();
            System.out.println("Successfully stopped the server.");
        }));

        server.awaitTermination();
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("grpc2kafka", options);
    }

    public static Server buildServer(final int serverPort, final boolean debug) {
        LOG.info("Starting NX-OS gRPC server without TLS on port {}...", serverPort);
        return ServerBuilder.forPort(serverPort)
                .addService(new NxosMdtDialoutService(debug))
                .build();
    }

}
