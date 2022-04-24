package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.TestClients;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

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


        final AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrap()));
        adminClient.createTopics(Set.of(new NewTopic("testtopic", 1, numBrokers).configs(Map.of("min.insync.replicas", "" + (numBrokers - 1))))).all().get();

        final var producer = TestClients.createProducer(kafkaCluster.getBootstrap(), Map.of(ProducerConfig.ACKS_CONFIG, "all", "retries", "10"));
        final var recordMetadata = producer.send(new ProducerRecord<>("testtopic", "first")).get();
        System.out.println("Send first message");
        assertThat(recordMetadata.offset()).isEqualTo(0);
        System.out.println("Stopping broker");
        kafkaCluster.kafkas.get(3).stop();

        final var recordMetadata1 = producer.send(new ProducerRecord<>("testtopic", "first")).get();
        System.out.println("Send second message " + recordMetadata1);
        assertThat(recordMetadata1.offset()).isEqualTo(1);

        System.out.println("Stopping broker");
        kafkaCluster.kafkas.get(2).stop();
        try {
            producer.send(new ProducerRecord<>("testtopic", "first")).get();
            fail("Should have failed with NotEnoughReplicasException");
        } catch (ExecutionException e) {
            if (! (e.getCause() instanceof NotEnoughReplicasException))
                throw e;
        }
    }
}
