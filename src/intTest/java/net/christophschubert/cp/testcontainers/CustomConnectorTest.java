package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.DataGenConfig;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class CustomConnectorTest {
    @Test
    public void customConnectorTest() throws IOException, InterruptedException {
        final CPTestContainerFactory factory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = factory.createKafka();
        kafka.start();
        final var connect = factory.createCustomConnector(Set.of("confluentinc/kafka-connect-s3:latest", "confluentinc/kafka-connect-datagen:0.4.0"), kafka);
        connect.start();

        final var topicName = "datagen";
        final int numMessages = 10;
        final var dataGenConfig = new DataGenConfig("datagen-connector")
                .withKafkaTopic(topicName)
                .withQuickstart("inventory")
                .withIterations(numMessages)
                .with("value.converter.schemas.enable", "false");


        final ConnectClient connectClient = new ConnectClient(connect.getBaseUrl());
        connectClient.startConnector(dataGenConfig);

        final TestClients.TestConsumer<String, String> consumer = TestClients.createConsumer(kafka.getBootstrapServers());
        consumer.subscribe(List.of(topicName));

        Assert.assertEquals(numMessages,  consumer.consumeUntil(numMessages).size());
    }
}
