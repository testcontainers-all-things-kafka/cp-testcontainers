package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static io.restassured.RestAssured.given;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.*;

public class AuditLogTest {
    @Test
    public void auditLogsAreEnabled() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final var cpServer = factory.createConfluentServer();

        final String admin = "admin";
        final String adminSecret = "admin-secret";
        final Map<String, String> userInfo = new HashMap<>();
        userInfo.put(admin, adminSecret);
        userInfo.put("bob", "bob-secret");

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

        cpServer
                //for basic authentication
//                        .withProperty("kafka.rest.authentication.method", "BASIC")
//                        .withProperty("kafka.rest.authentication.realm", "KafkaRest")
//                        .withProperty("kafka.rest.authentication.roles", "adminRole")
                //configure SASL Plain
//                .withProperty("kafka.rest.client.sasl.jaas.config", plainJaas(admin, adminSecret))
//                .withProperty("kafka.rest.client.security.protocol", SASL_PLAINTEXT)
//                .withProperty("kafka.rest.client.sasl.mechanism", PLAIN)
//


                //audit logs capture produce/consume events
//                .withProperty("confluent.security.event.router.config","{\"routes\":{\"crn:///kafka=*/group=*\":{\"consume\":{\"allowed\":\"confluent-audit-log-events\",\"denied\":\"confluent-audit-log-events\"}},\"crn:///kafka=*/topic=*\":{\"produce\":{\"allowed\":\"confluent-audit-log-events\",\"denied\":\"confluent-audit-log-events\"},\"consume\":{\"allowed\":\"confluent-audit-log-events\",\"denied\":\"confluent-audit-log-events\"}}},\"destinations\":{\"topics\":{\"confluent-audit-log-events\":{\"retention_ms\":7776000000}}},\"default_topics\":{\"allowed\":\"confluent-audit-log-events\",\"denied\":\"confluent-audit-log-events\"},\"excluded_principals\":[]}")
                .withProperty("confluent.security.event.router.config", logAllConf)

//                .withLogConsumer(o -> System.out.print(o))

                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", PLAIN)
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", PLAIN)
                .withEnv("KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS", PLAIN)
                .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG", plainJaas(admin, adminSecret, Map.of(admin, adminSecret)))
                .withEnv("KAFKA_SASL_JAAS_CONFIG", plainJaas(admin, adminSecret, Collections.emptyMap()))
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", plainJaas(admin, adminSecret, userInfo))
                .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer")
//                .withEnv("KAFKA_CONFLUENT_AUTHORIZER_ACCESS_RULE_PROVIDERS", "CONFLUENT")
                .withEnv("KAFKA_SUPER_USERS", "User:" + admin);

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

