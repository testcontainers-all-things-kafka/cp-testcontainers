package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.Collections;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

public class SchemaRegistryContainerTest {

    @Test
    public void setupSchemaRegistry() {

        final var testContainerFactory = new CPTestContainerFactory();

        final KafkaContainer kafka = testContainerFactory.createKafka();
        final SchemaRegistryContainer schemaRegistry = testContainerFactory.createSchemaRegistry(kafka);
        schemaRegistry.start(); //will implicitly start kafka container

        RestAssured.port = schemaRegistry.getMappedHttpPort();
        given().
                when().
                    get("/subjects").
                then().
                    statusCode(200).
                    body("", is(Collections.emptyList()));


        final var schemaRegistryUrl = schemaRegistry.getBaseUrl();
        final var topicName = "data.topic";

        final Producer<String, GenericRecord> producer = TestClients.createAvroProducer(kafka.getBootstrapServers(), schemaRegistryUrl);
        final Schema s = SchemaBuilder.builder().record("User").fields().requiredString("email").requiredInt("age").endRecord();
        final var originalRecord = new GenericRecordBuilder(s).set("email", "peter@a.com").set("age", 18).build();

        producer.send(new ProducerRecord<>(topicName, "user", originalRecord));
        producer.flush();

        final TestClients.TestConsumer<String, GenericRecord> consumer = TestClients.createAvroConsumer(kafka.getBootstrapServers(), schemaRegistryUrl);
        consumer.subscribe(List.of(topicName));
        final var genericRecords = consumer.consumeUntil(1);

        Assert.assertEquals(List.of(originalRecord), genericRecords);

        given().
                when().
                    get("/subjects").
                then().
                    statusCode(200).
                    body("", is(List.of(topicName + "-value")));

    }
}
