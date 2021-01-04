package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.ConnectorConfig;
import net.christophschubert.cp.testcontainers.util.DataGenConfig;
import net.christophschubert.cp.testcontainers.util.TestClients;
import net.christophschubert.cp.testcontainers.util.TestClients.TestConsumer;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;

public class KsqlDBContainerTest {

    @Test
    public void setupKsqlDB() {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var ksqlDB = containerFactory.createKsqlDB(kafka);
        ksqlDB.start();

        RestAssured.port = ksqlDB.getFirstMappedPort();

        given()
                .when()
                .get("/healthcheck")
                .then()
                .log().all()
                .statusCode(200)
                .body("isHealthy", is(true));
    }


    @Test
    public void setupKsqlDBWithFixedCustomImage() {
        final var tag = "0.14.0";
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var ksqlDB = containerFactory.createKsqDB(kafka, tag);
        ksqlDB.start();

        RestAssured.port = ksqlDB.getFirstMappedPort();

        given()
                .when()
                .get("/info")
                .then()
                .log().all()
                .statusCode(200)
                .body("KsqlServerInfo.serverStatus", is("RUNNING"))
                .body("KsqlServerInfo.version", startsWith(tag));
    }


    @Test
    public void setupKsqlDBWithSchemaRegistry() {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var schemaRegistry = containerFactory.createSchemaRegistry(kafka);
        schemaRegistry.start();

        final var serviceId = "test_ksqldb";
        final var ksqlDB = containerFactory
                .createKsqlDB(kafka)
                .withSchemaRegistry(schemaRegistry)
                .withServiceId(serviceId);
        ksqlDB.start();

        RestAssured.port = ksqlDB.getFirstMappedPort();

        given()
                .when()
                .get("/info")
                .then()
                .statusCode(200)
                .body("KsqlServerInfo.ksqlServiceId", is(serviceId));
    }


    @Test
    public void setupHeadlessKsqlDBWithSchemaRegistryAndConnect() throws IOException, InterruptedException, ExecutionException {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        final var schemaRegistry = containerFactory.createSchemaRegistry(kafka);
        final var connect = containerFactory.createCustomConnector("confluentinc/kafka-connect-datagen:0.4.0", kafka);

        Startables.deepStart(Stream.of(schemaRegistry, connect)).get();

        final var connectorName = "datagen-users";
        final ConnectorConfig connectorConfig = new DataGenConfig(connectorName)
                .withIterations(10000000)
                .withKafkaTopic("users")
                .withQuickstart("users")
                .withKeyConverter("org.apache.kafka.connect.storage.StringConverter")
                .withValueConverter("org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter.schemas.enable", false);

        ConnectClient connectClient = new ConnectClient(connect.getBaseUrl());
        connectClient.startConnector(connectorConfig);

        final var serviceId = "test_ksqldb";
        final var ksqlDB = containerFactory
                .createKsqlDB(kafka)
                .withSchemaRegistry(schemaRegistry)
                .withConnect(connect)
                .withQueriesFile("./src/intTest/resources/ksqlTest.sql")
                .withServiceId(serviceId)
                .withStartupTimeout(Duration.ofMinutes(5));
        ksqlDB.start();
        Assert.assertThat(connectClient.getConnectors(), is(Collections.singleton(connectorName)));

        final TestConsumer<String, GenericRecord> consumer = TestClients.createAvroConsumer(kafka.getBootstrapServers(), schemaRegistry.getBaseUrl());
        consumer.subscribe(List.of("users_avro"));

        var messages = consumer.consumeUntil(5);
        Assert.assertEquals(5, messages.size());
        Assert.assertNotNull(messages.get(0).get("USERID"));
    }
}
