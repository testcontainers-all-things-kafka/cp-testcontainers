package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.TestClients;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaClusterTest {

    @Test
    public void createCluster() {
        final short numBrokers = 4;
        final CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafkaCluster = factory.createKafkaCluster(numBrokers);
        kafkaCluster.startAll();

        TestClients.basicReadWriteTest(kafkaCluster.getBootstrap());
    }

    @Test
    public void bringDownBrokerUntilBelowMinISr() throws ExecutionException, InterruptedException {
        final short numBrokers = 4;
        final CPTestContainerFactory factory = new CPTestContainerFactory();
        final var kafkaCluster = factory.createKafkaCluster(numBrokers);
        kafkaCluster.startAll();


        AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrap()));
        adminClient.createTopics(Set.of(new NewTopic("testtopic", 1, numBrokers).configs(Map.of("min.insync.replicas", "" + (numBrokers - 1))))).all().get();

        final var producer = TestClients.createProducer(kafkaCluster.getBootstrap(), Map.of(ProducerConfig.ACKS_CONFIG, "all", "retries", "10"));
        final var recordMetadata = producer.send(new ProducerRecord<>("testtopic", "first")).get();
        System.out.println("Send first message");
        Assert.assertEquals(0, recordMetadata.offset());
        System.out.println("Stopping broker");
        kafkaCluster.kafkas.get(3).stop();

        final var recordMetadata1 = producer.send(new ProducerRecord<>("testtopic", "first")).get();
        System.out.println("Send second message " + recordMetadata1);
        Assert.assertEquals(1, recordMetadata1.offset());

        System.out.println("Stopping broker");
        kafkaCluster.kafkas.get(2).stop();
        try {
            producer.send(new ProducerRecord<>("testtopic", "first")).get();
            Assert.fail("Should have failed with NotEnoughReplicasException");
        } catch (ExecutionException e) {
            if (! (e.getCause() instanceof NotEnoughReplicasException))
                throw e;
        }
    }
}
