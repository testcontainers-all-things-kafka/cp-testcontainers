package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.TestClients;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    public void wrongPasswordRaisesException() throws InterruptedException {
        CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafka = factory.createKafkaSaslPlain(Map.of("bob", "bob-secret"));
        kafka.start();

        final var bobJaas = TestClients.createJaas("bob", "bob-wrongpassword");
        final Producer<String, String> producer = TestClients.createProducer(kafka.getBootstrapServers(), bobJaas);
        try {
            producer.send(new ProducerRecord<>("testtopic", "value")).get(); //should raise exception
        } catch (ExecutionException  e) {
            Assert.assertTrue(e.getCause() instanceof AuthenticationException);
            return;
        }
        Assert.fail("Should have exited with ExecutionException");
    }


    @Test
    public void setupWithSaslProducerConsumer() throws ExecutionException, InterruptedException {
        CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafka = factory.createKafkaSaslPlain(Map.of("bob", "bob-secret"));
        kafka.start();

        final var topicName = "testtopic";
        // a authenticated non-super user should be able to access topic
        final var jaas = TestClients.createJaas("bob", "bob-secret");
        final Producer<String, String> producer = TestClients.createProducer(kafka.getBootstrapServers(), jaas);
        producer.send(new ProducerRecord<>(topicName, "value")).get();


        final TestClients.TestConsumer<String, String> consumer = TestClients.createConsumer(kafka.getBootstrapServers(), jaas);
        consumer.subscribe(List.of(topicName));
        final var values = consumer.consumeUntil(1, Duration.ofSeconds(5), 2);

        Assert.assertEquals(1, values.size());
        Assert.assertEquals("value", values.get(0));
    }

    @Test
    public void superUserCanAccessWithoutAcls() throws ExecutionException, InterruptedException {
        CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafka = factory.createKafkaSaslPlain(Map.of("bob", "bob-secret"), true);
        kafka.start();

        final var topicName = "testtopic";
        // admin is a super user by default, we should still be able to access cluster even without ACLS
        final var adminJaas = TestClients.createJaas("admin", "admin-secret");
        final Producer<String, String> producer = TestClients.createProducer(kafka.getBootstrapServers(), adminJaas);
        producer.send(new ProducerRecord<>(topicName, "value")).get();


        final TestClients.TestConsumer<String, String> consumer = TestClients.createConsumer(kafka.getBootstrapServers(), adminJaas);
        consumer.subscribe(List.of(topicName));
        final var values = consumer.consumeUntil(1, Duration.ofSeconds(5), 2);

        Assert.assertEquals(1, values.size());
        Assert.assertEquals("value", values.get(0));

    }

    @Test
    public void noAclsShouldThrowException() throws InterruptedException {
        CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafka = factory.createKafkaSaslPlain(Map.of("bob", "bob-secret"), true);
        kafka.start();

        final var bobJaas = TestClients.createJaas("bob", "bob-secret");
        final Producer<String, String> producer = TestClients.createProducer(kafka.getBootstrapServers(), bobJaas);
        try {
            producer.send(new ProducerRecord<>("testtopic", "value")).get(); //should raise exception
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TopicAuthorizationException);
            return;
        }
        Assert.fail("Should have exited with ExecutionException");
    }

}
