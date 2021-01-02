package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.TestClients;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class KafkaContainerSaslTest {
    @Test(expected = ExecutionException.class)
    public void setupWithSaslProducerWithoutSaslFail() throws ExecutionException, InterruptedException {
        CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafka = factory.createKafkaSaslPlain(Collections.emptyMap());
        kafka.start();

        final Producer<String, String> producer = TestClients.createProducer(kafka.getBootstrapServers());
        producer.send(new ProducerRecord<>("testtopic", "value")).get(); // expected to fail as Jaas settings not configured
    }

    @Test
    public void setupWithSaslProducerConsumer() throws ExecutionException, InterruptedException {
        CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafka = factory.createKafkaSaslPlain(Collections.emptyMap());
        kafka.start();

        final var topicName = "testtopic";

        final var jaas = TestClients.createJaas("admin", "admin-secret");
        final Producer<String, String> producer = TestClients.createProducer(kafka.getBootstrapServers(), jaas);
        producer.send(new ProducerRecord<>(topicName, "value")).get();


        final TestClients.TestConsumer<String, String> consumer = TestClients.createConsumer(kafka.getBootstrapServers(), jaas);
        consumer.subscribe(List.of(topicName));
        final var values = consumer.consumeUntil(1, Duration.ofSeconds(5), 2);

        Assert.assertEquals(1, values.size());
        Assert.assertEquals("value", values.get(0));
    }

}
