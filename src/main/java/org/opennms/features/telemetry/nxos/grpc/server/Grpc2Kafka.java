package org.opennms.features.telemetry.nxos.grpc.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * The Class Grpc2Kafka.
 * 
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
public class Grpc2Kafka {

    /** The Constant LOG. */
    private static final Logger LOG = LoggerFactory.getLogger(Grpc2Kafka.class);

    /** The Constant DEFAULT_GRPC_PORT. */
    public static final int DEFAULT_GRPC_PORT = 50051;

    /** The Constant DEFAULT_KAFKA_BOOTSTRAP. */
    public static final String DEFAULT_KAFKA_BOOTSTRAP = "127.0.0.1:9092";

    /** The Constant DEFAULT_KAFKA_TOPIC. */
    public static final String DEFAULT_KAFKA_TOPIC = "OpenNMS.Sink.Telemetry-NXOS";

    /** The Metrics Registry. */
    private static final MetricRegistry metrics = new MetricRegistry();

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws InterruptedException the interrupted exception
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final Options options = new Options()
                .addOption("p", "port", true, "gRPC server listener port.\nDefault: " + DEFAULT_GRPC_PORT)
                .addOption("b", "bootstrap-servers", true, "Kafka bootstrap server list.\nDefault: " + DEFAULT_KAFKA_BOOTSTRAP)
                .addOption("t", "topic", true, "Kafka destination topic name.\nDefault: " + DEFAULT_KAFKA_TOPIC)
                .addOption("m", "minion-id", true, "OpenNMS Minion ID.")
                .addOption("l", "minion-location", true, "OpenNMS Minion Location.")
                .addOption("e", "producer-param", true, "Kafka Producer Parameters as key-value pairs.\nFor example: -e max.request.size=5000000")
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

        if (!cli.hasOption('l')) {
            System.err.println("ERROR: OpenNMS Minion Location is required.");
            printHelp(options);
            System.exit(0);
        }

        if (!cli.hasOption('m')) {
            System.err.println("ERROR: OpenNMS Minion ID is required.");
            printHelp(options);
            System.exit(0);
        }

        String kafkaServers = cli.hasOption('b') ? cli.getOptionValue('b') : DEFAULT_KAFKA_BOOTSTRAP;
        String kafkaTopic = cli.hasOption('t') ? cli.getOptionValue('t') : DEFAULT_KAFKA_TOPIC;
        Integer serverPort = cli.hasOption('p') ? new Integer(cli.getOptionValue('p')) : DEFAULT_GRPC_PORT;
        Properties producerProperties = new Properties();
        if (cli.hasOption('o')) {
            for (String option : cli.getOptionValues('o')) {
                String[] pair = option.split("=");
                producerProperties.setProperty(pair[0], pair[1]);
            }
        }

        KafkaProducer<String, byte[]> producer = buildProducer(kafkaServers, producerProperties);
        Server server = buildServer(serverPort, producer, kafkaTopic, cli.getOptionValue('m'), cli.getOptionValue('l'), cli.hasOption('d'));
        server.start();

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown request...");
            reporter.close();
            producer.close(10, TimeUnit.SECONDS);
            server.shutdown();
            System.out.println("Successfully stopped the gRPC server.");
        }));

        server.awaitTermination();
    }

    /**
     * Prints the help.
     *
     * @param options the options
     */
    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("grpc2kafka", options);
    }

    /**
     * Builds the Kafka producer.
     *
     * @param kafkaServers the Kafka servers bootstrap string
     * @return the Kafka producer
     */
    public static KafkaProducer<String, byte[]> buildProducer(final String kafkaServers, final Properties producerProperties) {
        Properties producerConfig = new Properties(producerProperties);
        producerConfig.setProperty("bootstrap.servers", kafkaServers);
        producerConfig.setProperty("key.serializer", StringSerializer.class.getName());
        producerConfig.setProperty("value.serializer", ByteArraySerializer.class.getName());
        producerConfig.setProperty("acks", "1");
        producerConfig.setProperty("retries", "3");
        LOG.info("Connecting to Kafka cluster using {}...", kafkaServers);
        return new KafkaProducer<String, byte[]>(producerConfig);
    }

    /**
     * Builds the gRPC server.
     *
     * @param serverPort the server port
     * @param producer the Kafka producer
     * @param kafkaTopic the Kafka topic
     * @param minionId the OpenNMS Minion id
     * @param minionLocation the OpenNMS Minion location
     * @param debug the debug flag
     * @return the gRPC server
     */
    public static Server buildServer(final int serverPort, final KafkaProducer<String, byte[]> producer, final String kafkaTopic, final String minionId, final String minionLocation, final boolean debug) {
        LOG.info("Starting NX-OS gRPC server without TLS on port {}...", serverPort);
        String ipAddress = "127.0.0.1";
        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("Cannot get host address because: ", e.getMessage());
        }
        return ServerBuilder.forPort(serverPort)
                .addService(new NxosMdtDialoutService(metrics, producer, kafkaTopic, minionId, minionLocation, ipAddress, serverPort, debug))
                .build();
    }

}
