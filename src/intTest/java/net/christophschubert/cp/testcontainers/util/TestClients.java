package net.christophschubert.cp.testcontainers.util;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import net.christophschubert.cp.testcontainers.CPTestContainerFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class TestClients {
    public static Map<String, Object> createJaas(String user, String password) {
        return Map.of(

                SaslConfigs.SASL_MECHANISM, "PLAIN",
                "security.protocol", "SASL_PLAINTEXT",
                SaslConfigs.SASL_JAAS_CONFIG, CPTestContainerFactory.formatJaas(user, password)
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
                    if (results.size() >= maxRecords)
                        return results;
                    results.add(r.value());
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

    public static TestConsumer<String, GenericRecord> createAvroConsumer(String bootstrapServer, String schemaRegistryUrl) {
        final var props = getCommonConsumerProps(bootstrapServer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        return new TestConsumer<>(props);
    }

    public static TestConsumer<String, String> createConsumer(String bootstrapServer, Map<String, Object> addProps) {
        final Map<String, Object> props = getCommonConsumerProps(bootstrapServer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putAll(addProps);
        return new TestConsumer<>(props);
    }
}
