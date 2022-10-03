package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.DataGenConfig;
import net.christophschubert.cp.testcontainers.util.TestClients;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class CustomConnectorTest {
    @Test
    public void customConnectorTest() throws IOException, InterruptedException {
        final CPTestContainerFactory factory = new CPTestContainerFactory();

        final String topicName;
        final int numMessages;
        final TestClients.TestConsumer<String, String> consumer;
        try (var kafka = factory.createKafka()) {
            final net.christophschubert.cp.testcontainers.util.ConnectorConfig dataGenConfig;
            final ConnectClient connectClient;
            try (var connect = factory.createCustomConnector(
                Set.of(
                    "confluentinc/kafka-connect-s3:latest",
                    "confluentinc/kafka-connect-datagen:0.4.0")
                , kafka)) {
                connect.start(); //implicitly starts kafka

                topicName = "datagen";
                numMessages = 10;
                dataGenConfig = new DataGenConfig("datagen-connector")
                    .withKafkaTopic(topicName)
                    .withQuickstart("inventory")
                    .withIterations(numMessages)
                    .with("value.converter.schemas.enable", "false");

                connectClient = new ConnectClient(connect.getBaseUrl());
            }
            connectClient.startConnector(dataGenConfig);

            consumer = TestClients.createConsumer(kafka.getBootstrapServers());
        }
        consumer.subscribe(List.of(topicName));

        assertThat(consumer.consumeUntil(numMessages).size()).isEqualTo(numMessages);
    }
}
