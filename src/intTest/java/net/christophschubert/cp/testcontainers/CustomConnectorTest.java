package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.ConnectorConfig;
import net.christophschubert.cp.testcontainers.util.ConsumerLoop;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class CustomConnectorTest {
    @Test
    public void customConnectorTest() throws IOException, InterruptedException {
        final CPTestContainerFactory factory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = factory.createKafka();
        kafka.start();
        final var connect = factory.createCustomConnector(Set.of("confluentinc/kafka-connect-s3:latest", "confluentinc/kafka-connect-datagen:0.4.0"), kafka);
//        connect.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        connect.start();

        final var topicName = "datagen";
        final int numMessages = 10;
        final var dataGenConfig = ConnectorConfig.source("datagen", "io.confluent.kafka.connect.datagen.DatagenConnector")
                .with("kafka.topic", topicName)
                .with("quickstart", "inventory")
                .with("instances", numMessages)
                .with("value.converter.schemas.enable", "false");

        final ConnectClient connectClient = new ConnectClient(connect.getBaseUrl());
        connectClient.startConnector(dataGenConfig);

        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(topicName));

        final var recordValues = ConsumerLoop.loopUntil(consumer, numMessages);
        Assert.assertEquals(numMessages, recordValues.size());
    }
}
