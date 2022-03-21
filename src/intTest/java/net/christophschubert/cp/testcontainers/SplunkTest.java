package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.ConnectorConfig;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;

import static net.christophschubert.cp.testcontainers.util.TestContainerUtils.startAll;

public class SplunkTest {

    public static class SplunkContainer extends GenericContainer<SplunkContainer> {

        public static final int port = 8000;
        public SplunkContainer() {
            super("splunk/splunk:latest");
            withExposedPorts(port, 8088);
            withEnv("SPLUNK_START_ARGS", "--accept-license");
            withEnv("SPLUNK_USERNAME", "admin");
            withEnv("SPLUNK_PASSWORD", "password");
            withEnv("DEBUG", "true");
            withFileSystemBind("src/intTest/resources/splunk-default.yml", "/tmp/defaults/default.yml");
        }

    }


    @Test
    public void startSplunk() throws InterruptedException, IOException {
        final Network network = Network.newNetwork();

        final SplunkContainer splunk = new SplunkContainer();
        splunk.withNetwork(network).withNetworkAliases("splunk");

        final CPTestContainerFactory factory = new CPTestContainerFactory(network);
        final KafkaContainer kafka = factory.createKafka();
        final var connect = factory.createCustomConnector("splunk/kafka-connect-splunk:2.0", kafka);
//        connect.withLogConsumer(o -> System.out.print(o.getUtf8String()));

        startAll(splunk, kafka, connect);

        final var topicName = "splunk-qs";
        final var producer = TestClients.createProducer(kafka.getBootstrapServers());

        producer.send(new ProducerRecord<>(topicName, "key1", "value1"));
        producer.send(new ProducerRecord<>(topicName, "key2", "value2"));
        producer.send(new ProducerRecord<>(topicName, "key3", "value3"));
        producer.send(new ProducerRecord<>(topicName, "key4", "value4"));
        producer.flush();

        System.out.println(splunk.getMappedPort(SplunkContainer.port));

        final ConnectClient client = new ConnectClient(connect.getBaseUrl());

        final var connectorConfig = ConnectorConfig.sink("splunk-con", "com.splunk.kafka.connect.SplunkSinkConnector")
                .with("topics", topicName)
                .with("splunk.hec.token", "99582090-3ac3-4db1-9487-e17b17a05081")
                .with("splunk.indexes", "main")
                .withValueConverter("org.apache.kafka.connect.storage.StringConverter")
                .withKeyConverter("org.apache.kafka.connect.storage.StringConverter")
                .with("splunk.sourcetypes", "my_sourcetype")
                .with("splunk.hec.raw", "false")
                .with("splunk.hec.uri", "http://splunk:8088");


        client.startConnector(connectorConfig);

        Thread.sleep(60_000);

        final var execResult = splunk.execInContainer("sudo splunk search 'source=http:splunk_hec_token' -auth 'admin:password'");
        System.out.println(execResult.toString());

    }


    @Test
    public void startSplunkCustomConnector() throws InterruptedException, IOException {
        final Network network = Network.newNetwork();

        final SplunkContainer splunk = new SplunkContainer();
        splunk.withNetwork(network).withNetworkAliases("splunk");

        final CPTestContainerFactory factory = new CPTestContainerFactory(network);
        final KafkaContainer kafka = factory.createKafka();
        final var connect = factory.createKafkaConnect(kafka)
        .withEnv("CONNECT_PLUGIN_PATH", "/extra")
                .withFileSystemBind("/Users/cschubert/git/christophschubert/kafka-connect-splunk/target/splunk-kafka-connect-v2.0.2.jar",
                        "/extra/splunk.jar");
        connect.withLogConsumer(o -> System.out.print(o.getUtf8String()));

        startAll(splunk, kafka, connect);

        final var topicName = "splunk-qs";
        final var producer = TestClients.createProducer(kafka.getBootstrapServers());

        producer.send(new ProducerRecord<>(topicName, "key1", "value1"));
        producer.send(new ProducerRecord<>(topicName, "key2", "value2"));
        producer.send(new ProducerRecord<>(topicName, "key3", "value3"));
        producer.send(new ProducerRecord<>(topicName, "key4", "value4"));
        producer.flush();

        System.out.println(splunk.getMappedPort(SplunkContainer.port));

        final ConnectClient client = new ConnectClient(connect.getBaseUrl());

        final var connectorConfig = ConnectorConfig.sink("splunk-con", "com.splunk.kafka.connect.SplunkSinkConnector")
                .with("topics", topicName)
                .with("splunk.hec.token", "99582090-3ac3-4db1-9487-e17b17a05081")
                .with("splunk.indexes", "main")
                .withValueConverter("org.apache.kafka.connect.storage.StringConverter")
                .withKeyConverter("org.apache.kafka.connect.storage.StringConverter")
                .with("splunk.sourcetypes", "my_sourcetype")
                .with("splunk.hec.raw", "false")
                .with("splunk.hec.uri", "http://splunk:8088");


        client.startConnector(connectorConfig);

        Thread.sleep(60_000);

        final var execResult = splunk.execInContainer("sudo splunk search 'source=http:splunk_hec_token' -auth 'admin:password'");
        System.out.println(execResult.toString());
    }
}




