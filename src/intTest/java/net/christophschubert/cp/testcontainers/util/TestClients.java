package net.christophschubert.cp.testcontainers.util;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.christophschubert.cp.testcontainers.SecurityConfigs;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TestClients {

    public static Map<String, Object> createJaas(String user, String password) {
        return Map.of(

                SaslConfigs.SASL_MECHANISM, SecurityConfigs.PLAIN,
                "security.protocol", SecurityConfigs.SASL_PLAINTEXT,
                SaslConfigs.SASL_JAAS_CONFIG, SecurityConfigs.plainJaas(user, password)
        );
    }

    public static Producer<String, String> createProducer(String bootstrapServer) {
        return createProducer(bootstrapServer, Collections.emptyMap());
    }

    public static Producer<String, String> createProducer(String bootstrapServer, Map<String, Object> addProps) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.putAll(addProps);
        return new KafkaProducer<>(props);
    }

    public static class TestConsumer<K, V> extends KafkaConsumer<K, V> {
        public TestConsumer(Map<String, Object> configs) {
            super(configs);
        }

        public List<V> consumeUntil(int maxRecords) {
            return consumeUntil(maxRecords, Duration.ofSeconds(1), 60);
        }

        public List<V> consumeUntil(int maxRecords, Duration timeout, int tries) {
            final List<V> results = new ArrayList<>();
            for (int i = 0; i < tries; ++i) {
                final var records = poll(timeout);
                for (ConsumerRecord<K, V> r : records) {
                    results.add(r.value());
                    if (results.size() >= maxRecords)
                        return results;
                }
            }
            return results;
        }
    }

    public static TestConsumer<String, String> createConsumer(String bootstrapServer) {
        return createConsumer(bootstrapServer, Collections.emptyMap());
    }

    static Map<String, Object> getCommonConsumerProps(String bootstrap) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }


    public static TestConsumer<String, String> createConsumer(String bootstrapServer, Map<String, Object> addProps) {
        final Map<String, Object> props = getCommonConsumerProps(bootstrapServer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putAll(addProps);
        return new TestConsumer<>(props);
    }

    public static TestConsumer<String, GenericRecord> createAvroConsumer(String bootstrapServer, String schemaRegistryUrl) {
        return createAvroConsumer(bootstrapServer, schemaRegistryUrl, Collections.emptyMap());
    }

    public static TestConsumer<String, GenericRecord> createAvroConsumer(String bootstrapServer, String schemaRegistryUrl, Map<String, Object> addProps) {
        final var props = getCommonConsumerProps(bootstrapServer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.putAll(addProps);
        return new TestConsumer<>(props);
    }

    public static Producer<String, GenericRecord> createAvroProducer(String bootstrapServer, String schemaRegistryUrl) {
        return createAvroProducer(bootstrapServer, schemaRegistryUrl, Collections.emptyMap());
    }

    public static Producer<String, GenericRecord> createAvroProducer(String bootstrapServer, String schemaRegistryUrl, Map<String, Object> addProps) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.putAll(addProps);
        return new KafkaProducer<>(props);
    }

    static public void basicReadWriteTest(String bootStrapServer) {
        final var producer = TestClients.createProducer(bootStrapServer);
        final var topic = "test-topic";
        final var value = "hello-world";
        try {
            producer.send(new ProducerRecord<>(topic, value)).get();

            final var consumer = createConsumer(bootStrapServer);
            consumer.subscribe(Set.of(topic));
            final var results = consumer.consumeUntil(1);
            System.out.println("RESULTS " + results);
            Assert.assertEquals(1, results.size());
            Assert.assertEquals(value, results.get(0));
        } catch (InterruptedException | ExecutionException e) {
            throw new AssertionError("Basic Kafka test failed", e);
        }
    }
}
