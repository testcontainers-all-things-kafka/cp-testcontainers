package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.ConnectorConfig;
import net.christophschubert.cp.testcontainers.util.LogWaiter;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ReplicatorConnectorTest {
    @Test
    public void setupReplicator() throws InterruptedException, IOException, ExecutionException {
        final Network network = Network.newNetwork();
        final var testContainerFactory = new CPTestContainerFactory(network);


        final KafkaContainer sourceKafka = testContainerFactory.createKafka();
        final KafkaContainer destinationKafka = testContainerFactory.createKafka();
        sourceKafka.start();
        destinationKafka.start();

        final LogWaiter waiter = new LogWaiter("INFO Successfully started up Replicator source task");

        final KafkaConnectContainer replicatorContainer = testContainerFactory.createReplicator(destinationKafka);
        replicatorContainer.withLogConsumer(outputFrame -> waiter.accept(outputFrame.getUtf8String()));
        replicatorContainer.start();

        //pre-create topics:
        final AdminClient adminClient = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers()));
        adminClient.createTopics(List.of(new NewTopic("data.topic", Optional.empty(), Optional.empty()))).all().get();

        final var replicatorConfig = ConnectorConfig.source("replicator-data", "io.confluent.connect.replicator.ReplicatorSourceConnector")
                .withTopicRegex("data\\..*")
                .with("topic.rename.format", "${topic}.replica")
                .withKeyConverter("io.confluent.connect.replicator.util.ByteArrayConverter")
                .withValueConverter("io.confluent.connect.replicator.util.ByteArrayConverter")
                .with("src.kafka.bootstrap.servers", CPTestContainer.getInternalBootstrap(sourceKafka));

        final ConnectClient connectClient = new ConnectClient(replicatorContainer.getBaseUrl());
        connectClient.startConnector(replicatorConfig);


        final Producer<String, String> producer = TestClients.createProducer(sourceKafka.getBootstrapServers());

        final String testValue = "some-value";
        producer.send(new ProducerRecord<>("data.topic", "user", testValue));
        producer.flush();

        final Consumer<String, String> consumer = TestClients.createConsumer(destinationKafka.getBootstrapServers());
        consumer.subscribe(List.of("data.topic.replica"));

        var msgCount = 0;

        while(!waiter.found) {
            for (int i = 0; i < 2; i++) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                    Assert.assertEquals(testValue, record.value());
                    ++msgCount;
                }
            }
        }
        Assert.assertEquals(1, msgCount);
    }

    // quick test for configuring connectors
    @Test
    public void setupReplicatorForJson() throws InterruptedException, IOException, ExecutionException {
        final Network network = Network.newNetwork();
        final var testContainerFactory = new CPTestContainerFactory(network);

        final KafkaContainer sourceKafka = testContainerFactory.createKafka();
        final KafkaContainer destinationKafka = testContainerFactory.createKafka();
        sourceKafka.start();
        destinationKafka.start();

        final LogWaiter waiter = new LogWaiter("INFO Successfully started up Replicator source task");

        final KafkaConnectContainer replicatorContainer = testContainerFactory.createReplicator(destinationKafka);
        replicatorContainer.withLogConsumer(outputFrame -> waiter.accept(outputFrame.getUtf8String()));
        replicatorContainer.start();

        //pre-create topics:
        final AdminClient adminClient = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers()));
        adminClient.createTopics(List.of(new NewTopic("data.topic", Optional.empty(), Optional.empty()))).all().get();

        final var replicatorConfig = ConnectorConfig.source("replicator-data", "io.confluent.connect.replicator.ReplicatorSourceConnector")
                .withTopicRegex("data\\..*")
                .with("topic.rename.format", "${topic}.replica")
                .with("src.value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("src.value.converter.schemas.enable", false)
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter") // need to write this in this test case even though it's the default in Connect cluster in order for
                .with("value.converter.schemas.enable", false)
                .with("src.kafka.bootstrap.servers", CPTestContainer.getInternalBootstrap(sourceKafka));

        final ConnectClient connectClient = new ConnectClient(replicatorContainer.getBaseUrl());
        connectClient.startConnector(replicatorConfig);


        final Producer<String, String> producer = TestClients.createProducer(sourceKafka.getBootstrapServers());
        final Consumer<String, String> consumer = TestClients.createConsumer(destinationKafka.getBootstrapServers());
        consumer.subscribe(List.of("data.topic.replica"));

        final String testValue = "{\"key1\":\"value1\",\"key2\":12}";
        producer.send(new ProducerRecord<>("data.topic", "user", testValue));
        producer.flush();

        var msgCount = 0;

        while(!waiter.found) {
            for (int i = 0; i < 2; i++) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                    Assert.assertEquals(testValue, record.value());
                    ++msgCount;
                }
            }

        }
        Assert.assertEquals(1, msgCount);
    }

}
