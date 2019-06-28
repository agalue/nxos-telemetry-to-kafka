package org.opennms.features.telemetry.nxos.grpc.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The Class Grpc2Kafka.
 * 
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
@Command(name="grpc2kafka", mixinStandardHelpOptions=true, version="1.0.0")
public class Grpc2Kafka implements Runnable {

    /** The Constant LOG. */
    private static final Logger LOG = LoggerFactory.getLogger(Grpc2Kafka.class);

    @Option(names={"-b","--bootstrap-servers"}, paramLabel="server", description="Kafka bootstrap server list.\nExample: kafka1:9092", required=true)
    String kafkaServers;

    @Option(names={"-p","--port"}, paramLabel="port", description="gRPC server listener port.\nDefault: ${DEFAULT-VALUE}", defaultValue="50051")
    int serverPort;

    @Option(names={"-t","--topic"}, paramLabel="topic", description="Kafka destination topic name.\nDefault: ${DEFAULT-VALUE}", defaultValue="OpenNMS.Sink.Telemetry-NXOS")
    String kafkaTopic;

    @Option(names={"-m","--minion-id"}, paramLabel="id", description="OpenNMS Minion ID", required=true)
    String minionId;

    @Option(names={"-l","--minion-location"}, paramLabel="location", description="OpenNMS Minion Loaction", required=true)
    String minionLocation;

    @Option(names={"-e","--producer-param"}, paramLabel="param", split=",", description="Optional Kafka Producer parameters as comma separated list of key-value pairs.\nExample: -e max.request.size=5000000,acks=1")
    List<String> producerParameters;

    @Option(names={"-M","--max-buffer-size"}, paramLabel="size", description="The maximum size in bytes of the message buffer chunk, used to split big messages into multiple ones and sent them to the same partition in Kafka.\nDefault: ${DEFAULT-VALUE} (disabled)", defaultValue="0")
    Integer maxBufferSize;

    @Option(names={"-d","--debug"}, description="Show message on logs")
    boolean debug;

    /** The Metrics Registry. */
    private final MetricRegistry metrics = new MetricRegistry();

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws InterruptedException the interrupted exception
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        Grpc2Kafka app = CommandLine.populateCommand(new Grpc2Kafka(), args);
        CommandLine.run(app, args);
    }

    @Override
    public void run() {
        try {
            final Properties producerProperties = new Properties();
            if (producerParameters != null) {
                producerParameters.forEach(option -> {
                    String[] pair = option.split("=");
                    producerProperties.setProperty(pair[0], pair[1]);
                });
            }

            KafkaProducer<String, byte[]> producer = buildProducer(kafkaServers, producerProperties);
            Server server = buildServer(producer);
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
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Builds the Kafka producer.
     *
     * @param kafkaServers the Kafka servers bootstrap string
     * @return the Kafka producer
     */
    public KafkaProducer<String, byte[]> buildProducer(final String kafkaServers, final Properties producerProperties) {
        Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers", kafkaServers);
        producerConfig.setProperty("key.serializer", StringSerializer.class.getName());
        producerConfig.setProperty("value.serializer", ByteArraySerializer.class.getName());
        producerConfig.setProperty("acks", "1");
        producerConfig.setProperty("retries", "3");
        producerConfig.putAll(producerProperties);
        LOG.info("Connecting to Kafka cluster using {}...", kafkaServers);
        return new KafkaProducer<String, byte[]>(producerConfig);
    }

    /**
     * Builds the gRPC server.
     *
     * @param producer the Kafka producer
     * @return the gRPC server
     */
    public Server buildServer(final KafkaProducer<String, byte[]> producer) {
        LOG.info("Starting NX-OS gRPC server without TLS on port {}...", serverPort);
        String ipAddress = "127.0.0.1";
        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOG.warn("Cannot get host address because: ", e.getMessage());
        }
        return ServerBuilder.forPort(serverPort)
                .addService(new NxosMdtDialoutService(metrics, producer, kafkaTopic, minionId, minionLocation, ipAddress, serverPort, maxBufferSize, debug))
                .build();
    }

}
