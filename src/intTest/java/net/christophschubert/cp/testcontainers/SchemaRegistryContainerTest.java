package net.christophschubert.cp.testcontainers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SchemaRegistryContainerTest {
    @Test
    public void setupSchemaRegistry() throws IOException, InterruptedException {

        final var testContainerFactory = new CPTestContainerFactory(Network.newNetwork());

        final KafkaContainer sourceKafka = testContainerFactory.createKafka();
        sourceKafka.start();

        final SchemaRegistryContainer sourceSchemaRegistry = testContainerFactory.createSchemaRegistry(sourceKafka);
        sourceSchemaRegistry.start();

        final HttpClient client = HttpClient.newBuilder().build();

        final var schemaRegistryUrl = sourceSchemaRegistry.getBaseUrl();
        final var request = HttpRequest.newBuilder(URI.create(schemaRegistryUrl + "/subjects")).build();
        final var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        Assert.assertEquals(200, response.statusCode());
        Assert.assertEquals("[]", response.body());


        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final Producer<String, GenericRecord> producer = new KafkaProducer<>(producerProperties);


        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProperties);

        final Schema s = SchemaBuilder.builder().record("User").fields().requiredString("email").requiredInt("age").endRecord();
        final var originalRecord = new GenericRecordBuilder(s).set("email", "peter@a.com").set("age", 18).build();
        producer.send(new ProducerRecord<>("data.topic", "user", originalRecord));
        producer.flush();

        consumer.subscribe(List.of("data.topic"));

        final ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, GenericRecord> record : records) {
            Assert.assertEquals(originalRecord, record.value());
        }
    }
}
