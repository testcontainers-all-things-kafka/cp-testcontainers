package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.TestClients;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static net.christophschubert.cp.testcontainers.SecurityConfigs.plainJaasProperties;

public class AuditLogTest {
    @Test
    public void auditLogsAreEnabled() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final var cpServer = factory.createConfluentServer();

        final var logAllConf =
                "{" +
                        "        \"routes\": {" +
                        "                  \"crn:///kafka=*\": {" +
                        "                            \"interbroker\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }," +
                        "                            \"describe\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }," +
                        "                            \"management\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }" +
                        "                  }," +
                        "                  \"crn:///kafka=*/group=*\": {" +
                        "                            \"consume\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }," +
                        "                            \"describe\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }," +
                        "                            \"management\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }" +
                        "                  }," +
                        "                  \"crn:///kafka=*/topic=*\": {" +
                        "                            \"produce\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }," +
                        "                            \"consume\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }," +
                        "                            \"describe\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }," +
                        "                            \"management\": {" +
                        "                                      \"allowed\": \"confluent-audit-log-events\"," +
                        "                                      \"denied\": \"confluent-audit-log-events\"" +
                        "                            }" +
                        "                  }," +
                        "                  \"crn:///kafka=*/topic=_*\": {" +
                        "                            \"produce\": {" +
                        "                                      \"allowed\": \"\"," +
                        "                                      \"denied\": \"\"" +
                        "                            }," +
                        "                            \"consume\": {" +
                        "                                      \"allowed\": \"\"," +
                        "                                      \"denied\": \"\"" +
                        "                            }," +
                        "                            \"describe\": {" +
                        "                                      \"allowed\": \"\"," +
                        "                                      \"denied\": \"\"" +
                        "                            }" +
                        "                  }" +
                        "        }," +
                        "        \"destinations\": {" +
                        "                  \"topics\": {" +
                        "                            \"confluent-audit-log-events\": {" +
                        "                                      \"retention_ms\": 7776000000" +
                        "                            }" +
                        "                  }" +
                        "        }," +
                        "        \"default_topics\": {" +
                        "                  \"allowed\": \"confluent-audit-log-events\"," +
                        "                  \"denied\": \"confluent-audit-log-events\"" +
                        "        }" +
                        "}";

        SalsPlainDecorator decorator = new SalsPlainDecorator(Map.of("bob", "bob-secret"));
        decorator.addSaslPlainConfig(cpServer, true);
        cpServer.withProperty("confluent.security.event.router.config", logAllConf);


        cpServer.start();


        final var topicName = "testtopic";
        // admin is a super user by default, we should still be able to access cluster even without ACLS
        final var adminJaas = TestClients.createJaas("admin", "admin-secret");
        final Producer<String, String> producer = TestClients.createProducer(cpServer.getBootstrapServers(), adminJaas);
        producer.send(new ProducerRecord<>(topicName, "value")).get();

        final var bobProperties = plainJaasProperties("bob", "bob-secret");
        final TestClients.TestConsumer<String, String> consumer = TestClients.createConsumer(cpServer.getBootstrapServers(), bobProperties);
        consumer.subscribe(List.of(topicName));
        try {
            final var values = consumer.consumeUntil(1000, Duration.ofSeconds(5), 5);
        } catch (Exception e) {
            // expect exception: bob isn't authorized to read from topic
            e.printStackTrace();
        } finally {
            consumer.close();
        }

        try {
            final Producer<String, String> bobProducer = TestClients.createProducer(cpServer.getBootstrapServers(), bobProperties);
            bobProducer.send(new ProducerRecord<>("bob-isnt-allowed-to-write-here", "value")).get();
        } catch (Exception e) {
            // expect exception: bob isn't authorized to write to topic
            e.printStackTrace();
        }

        System.out.println("started CP server");

        final TestClients.TestConsumer<String, String> auditLogConsumer = TestClients.createConsumer(cpServer.getBootstrapServers(), adminJaas);
        auditLogConsumer.subscribe(List.of("confluent-audit-log-events"));
        final var logEvents = auditLogConsumer.consumeUntil(1000, Duration.ofSeconds(2), 5);
        logEvents.forEach(System.out::println);
    }


}

