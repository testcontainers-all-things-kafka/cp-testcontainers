package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.ConsumerLoop;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaContainerSaslTest {
    @Test(expected = ExecutionException.class)
    public void setupWithSaslProducerWithoutSaslFail() throws ExecutionException, InterruptedException {
        CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafka = factory.createKafkaSaslPlain(Collections.emptyMap());
        kafka.start();

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

        final Producer<String, String> producer = new KafkaProducer<>(producerProperties);
        producer.send(new ProducerRecord<>("testtopic", "value")).get(); // expected to fail as Jaas settings not configured
    }

    @Test
    public void setupWithSaslProducerConsumer() throws ExecutionException, InterruptedException {
        CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafka = factory.createKafkaSaslPlain(Collections.emptyMap());

        kafka.start();

        final var topicName = "testtopic";

        final var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        producerProperties.put("sasl.mechanism", "PLAIN");
        producerProperties.put("security.protocol", "SASL_PLAINTEXT");
        producerProperties.put("sasl.jaas.config", CPTestContainerFactory.formatJaas("admin", "admin-secret"));

        final Producer<String, String> producer = new KafkaProducer<>(producerProperties);
        producer.send(new ProducerRecord<>(topicName, "value")).get();


        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerProperties.put("sasl.mechanism", "PLAIN");
        consumerProperties.put("security.protocol", "SASL_PLAINTEXT");
        consumerProperties.put("sasl.jaas.config", CPTestContainerFactory.formatJaas("admin", "admin-secret"));

        final Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(topicName));
        final var values = ConsumerLoop.loopUntil(consumer, 1, Duration.ofSeconds(5), 2);

        Assert.assertEquals(1, values.size());
        Assert.assertEquals("value", values.get(0));
    }

}
