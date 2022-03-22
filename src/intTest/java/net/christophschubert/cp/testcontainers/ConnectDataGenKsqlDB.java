package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.DataGenConfig;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

public class ConnectDataGenKsqlDB {

    @Test
    public void setupKsqlDBWithSchemaRegistry() throws IOException, InterruptedException {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var schemaRegistry = containerFactory.createSchemaRegistry(kafka);
        schemaRegistry.start();

        final var serviceId = "test_ksqldb";
        final var ksqlDB = containerFactory
                .createKsqDB(kafka, "0.17.0")
                .withSchemaRegistry(schemaRegistry)
                .withServiceId(serviceId);
        ksqlDB.start();

        final var connect = containerFactory.createCustomConnector(
                Set.of("confluentinc/kafka-connect-datagen:0.4.0")
                , kafka);
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


        RestAssured.port = ksqlDB.getFirstMappedPort();
        
        given()
                .when()
                .get("/info")
                .then()
                .statusCode(200)
                .body("KsqlServerInfo.ksqlServiceId", is(serviceId));
    }
}
