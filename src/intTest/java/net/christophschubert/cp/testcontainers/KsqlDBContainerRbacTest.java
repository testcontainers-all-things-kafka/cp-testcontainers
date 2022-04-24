package net.christophschubert.cp.testcontainers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.christophschubert.cp.testcontainers.util.MdsRestWrapper;
import net.christophschubert.cp.testcontainers.util.TestClients;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import io.restassured.RestAssured;

import static io.restassured.RestAssured.given;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ClusterType.KsqlCluster;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.KafkaResourceType.Cluster;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.KafkaResourceType.Group;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.KafkaResourceType.Topic;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.KafkaResourceType.TransactionalId;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ResourceRole.DeveloperRead;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ResourceRole.DeveloperWrite;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ResourceRole.ResourceOwner;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ResourceType.KsqlClusterResource;
import static net.christophschubert.cp.testcontainers.util.TestContainerUtils.startAll;
import static org.hamcrest.CoreMatchers.is;

@Disabled
public class KsqlDBContainerRbacTest {

    static final String alice = "alice";
    static final String aliceSecret = "alice-secret";
    static final String ksql = "ksql";
    static final String ksqlSecret = "ksql-secret";

    static final String ksqlMediaType = "application/vnd.ksql.v1+json";

    // starts a RBAC enabled CP server instance and ksql with minimal role bindings for it to start up.
    @Test
    public void setupKsqlDB() {

        final var containerFactory = new CPTestContainerFactory();
        final var ldap = containerFactory.createLdap(Set.of(alice, ksql));
        // users:
        //      alice is our trusted super-user
        //      ksql is the technical user for the ksqlDB cluster
        final var kafka = containerFactory.createConfluentServer().enableRbac();
        startAll(ldap, kafka);

        final var ksqlDB = containerFactory.createKsqlDB(kafka)
                .enableRbac(kafka.getMdsUrl(), ksql, ksqlSecret);
        ksqlDB.withServiceId("ksql");
        ksqlDB.withStartupTimeout(Duration.ofMinutes(1));

        // configure role-bindings, alice is a super.user
        final var mdsWrapper = new MdsRestWrapper(kafka.getMdsPort(), alice, aliceSecret);

        mdsWrapper.grantRoleOnCluster(ksql, MdsRestWrapper.ClusterRole.SecurityAdmin, KsqlCluster, "ksql");
        mdsWrapper.grantRoleOnResource(ksql , ResourceOwner, KsqlCluster, "ksql-cluster", MdsRestWrapper.ResourceType.KsqlCluster, "ksql-cluster");
        // using `ksql-cluster` for the clusterId might be wrong, should it just be `ksql` (the service Id)? Double-check in documentation.

        // it seems to me that the REST calls on https://docs.confluent.io/platform/current/security/rbac/rbac-config-using-rest-api.html
        // are not correct for ksql

        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Group, "_confluent-ksql-ksql", true);
        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Topic, "_confluent-ksql-ksql", true);
        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Topic, "ksqlksql_processing_log");
        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Topic, "ksql", true); // should change this

        ksqlDB.start();

        RestAssured.port = ksqlDB.getFirstMappedPort();
        given()
                .when()
                .get("/healthcheck")
                .then()
                .log().all()
                .statusCode(200)
                .body("isHealthy", is(true));
    }

    @Test
    public void sanityCheckWithoutRbac() throws JsonProcessingException, ExecutionException, InterruptedException {
        //set up CP Server with RBAC, configure ksqlDB server to perform simple stateful query.
        final var containerFactory = new CPTestContainerFactory();
        // users:
        //      alice is our trusted super-user
        //      bob: wants to start interactive queries
        //      ksql is the technical user for the ksqlDB cluster
        //      producer, consumer: generate input/output data
        final var ldap = containerFactory.createLdap(Set.of("alice", "bob", "ksql", "consumer", "producer"));

        final var kafka = containerFactory.createConfluentServer();
        startAll(ldap, kafka);

        final String ksqlClusterId = "uppercase-ksql";
        final var ksqlDB = containerFactory.createKsqlDB(kafka);
        ksqlDB.withServiceId(ksqlClusterId);
        ksqlDB.withStartupTimeout(Duration.ofMinutes(1));


        final String inputTopic = "orders";

        ksqlDB.start();
        RestAssured.port = ksqlDB.getFirstMappedPort();
        given()
                .when()
                .get("/healthcheck")
                .then()
                .log().all()
                .statusCode(200)
                .body("isHealthy", is(true));

        final String bob = "bob";

        final var producer = TestClients.createProducer(kafka.getBootstrapServers());

        ObjectMapper mapper = new ObjectMapper();

        producer.send(new ProducerRecord<>(inputTopic, "milk", mapper.writeValueAsString(Map.of("product", "milk", "quantity", 10))));
        producer.send(new ProducerRecord<>(inputTopic, "juice", mapper.writeValueAsString(Map.of("product", "juice", "quantity", 5))));
        producer.send(new ProducerRecord<>(inputTopic, "milk", mapper.writeValueAsString(Map.of("product", "milk", "quantity", 8))));
        producer.send(new ProducerRecord<>(inputTopic, "juice", mapper.writeValueAsString(Map.of("product", "juice", "quantity", 7)))).get();
        producer.flush();

        final var body = Map.of("ksql", "CREATE STREAM orders (product varchar, quantity bigint) WITH (KAFKA_TOPIC = 'orders', VALUE_FORMAT = 'JSON');");

        given()
                .auth().preemptive().basic(bob, "bob-secret")
                .accept(ksqlMediaType)
                .contentType(ksqlMediaType)
                .body(body)
                .when()
                    .post("/ksql")
                .then()
                    .log().all();
    }

    @Test
    public void simpleStream() throws JsonProcessingException, ExecutionException, InterruptedException {
        //set up CP Server with RBAC, configure ksqlDB server to perform simple stateful query.
        final var containerFactory = new CPTestContainerFactory();
        // users:
        //      alice is our trusted super-user
        //      bob: wants to start interactive queries
        //      ksql is the technical user for the ksqlDB cluster
        //      producer, consumer: generate input/output data
        final var ldap = containerFactory.createLdap(Set.of("alice", "bob", "ksql", "consumer", "producer"));

        final var kafka = containerFactory.createConfluentServer().enableRbac();
        startAll(ldap, kafka);

        final String ksqlClusterId = "uppercase-ksql";
        final var ksqlDB = containerFactory.createKsqlDB(kafka)
                .enableRbac(kafka.getMdsUrl(), ksql, ksqlSecret);
        ksqlDB.withServiceId(ksqlClusterId);
        ksqlDB.withStartupTimeout(Duration.ofMinutes(1));

        // configure role-bindings, alice is a super.user
        final var mdsWrapper = new MdsRestWrapper(kafka.getMdsPort(), alice, aliceSecret);

        final String inputTopic = "orders";
        final String outputTopic = "output";

        //grant for producer/consumer
        mdsWrapper.grantRoleOnKafkaResource("producer", ResourceOwner, Topic, inputTopic); // ResourceOwner to allow topic creation
        mdsWrapper.grantRoleOnKafkaResource("consumer", DeveloperRead, Topic, outputTopic);

        // setting up role-bindings according to https://docs.confluent.io/platform/current/security/rbac/ksql-rbac.html
        mdsWrapper.grantRoleOnCluster(ksql, MdsRestWrapper.ClusterRole.SecurityAdmin, KsqlCluster, ksqlClusterId);
        mdsWrapper.grantRoleOnResource(ksql , ResourceOwner, KsqlCluster, ksqlClusterId, MdsRestWrapper.ResourceType.KsqlCluster, "ksql-cluster");

        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Group, "_confluent-ksql-" + ksqlClusterId, true);
        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Topic, String.format("_confluent-ksql-%s_command_topic", ksqlClusterId));
        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Topic, ksqlClusterId + "ksql_processing_log");
        mdsWrapper.grantRoleOnKafkaResource(ksql, DeveloperWrite, TransactionalId, ksqlClusterId);
        mdsWrapper.grantRoleOnKafkaResource(ksql, DeveloperWrite, Cluster, "kafka-cluster");

        ksqlDB.start();
        RestAssured.port = ksqlDB.getFirstMappedPort();
        given()
                .when()
                .get("/healthcheck")
                .then()
                .log().all()
                .statusCode(200)
                .body("isHealthy", is(true));

        // assign roles for interactive queries to bob: bob wants to perform an aggregation of data from the inputTopic

        final String bob = "bob";

        //Grant write access to resources on the ksqlDB cluster.
        mdsWrapper.grantRoleOnResource(bob, DeveloperWrite, KsqlCluster, ksqlClusterId, KsqlClusterResource, "ksql-cluster");

        //Grant read-only access to the ksqlDB consumer groups.
        mdsWrapper.grantRoleOnKafkaResource(bob, DeveloperRead, Group, "_confluent-ksql-" + ksqlClusterId, true);

        // Added these two bindings to make aggregation work
        // TODO: look into why they are necessary
        mdsWrapper.grantRoleOnKafkaResource(bob, ResourceOwner, Topic, "_confluent-ksql-" + ksqlClusterId, true); // without this, aggregation crashes
        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Topic, "_confluent-ksql-" + ksqlClusterId, true); // without this, aggregation crashes

        //Grant access  to the processing log: this is optional:
        mdsWrapper.grantRoleOnKafkaResource(bob, DeveloperRead, Topic, ksqlClusterId + "ksql_processing_log");

        //seems that we need to grant rights both the the user and the cluster to read/write to all input/output topics
        // TODO: doublecheck!
        mdsWrapper.grantRoleOnKafkaResource(bob, DeveloperRead, Topic, inputTopic);
        mdsWrapper.grantRoleOnKafkaResource(ksql, DeveloperRead, Topic, inputTopic);
        mdsWrapper.grantRoleOnKafkaResource(bob, ResourceOwner, Topic, outputTopic); // to write and create topic, can we get by with DeveloperWrite if we created the topic beforehand?
        mdsWrapper.grantRoleOnKafkaResource(ksql, ResourceOwner, Topic, outputTopic); // to write and create topic, can we get by with DeveloperWrite if we created the topic beforehand?


        final var producer = TestClients.createProducer(
                kafka.getBootstrapServers(),
                SecurityConfigs.plainJaasProperties("producer", "producer-secret")
        );

        ObjectMapper mapper = new ObjectMapper();

        producer.send(new ProducerRecord<>(inputTopic, "milk", mapper.writeValueAsString(Map.of("product", "milk", "quantity", 10))));
        producer.send(new ProducerRecord<>(inputTopic, "juice", mapper.writeValueAsString(Map.of("product", "juice", "quantity", 5))));
        producer.send(new ProducerRecord<>(inputTopic, "milk", mapper.writeValueAsString(Map.of("product", "milk", "quantity", 8))));
        final var recordMetadata = producer.send(new ProducerRecord<>(inputTopic, "juice", mapper.writeValueAsString(Map.of("product", "juice", "quantity", 7)))).get();
        System.out.println(recordMetadata);
        producer.flush();



        final var body = Map.of("ksql", "CREATE STREAM orders (product varchar, quantity bigint) WITH (KAFKA_TOPIC = 'orders', VALUE_FORMAT = 'JSON');",
                "streamsProperties", Map.of("ksql.streams.auto.offset.reset", "earliest"));

        given()
                .auth().preemptive().basic(bob, bob + "-secret")
                .accept(ksqlMediaType)
                .contentType(ksqlMediaType)
                .body(body)
                .when()
                    .post("/ksql")
                .then()
                    .log().all()
                    .statusCode(200);

        final var body2 = Map.of("ksql", "CREATE table aggs WITH (KAFKA_TOPIC = 'output', VALUE_FORMAT = 'JSON', partitions=1) as select product, sum(quantity) from orders group by product ;",
                "streamsProperties", Map.of("ksql.streams.auto.offset.reset", "earliest"));

        given()
                .auth().preemptive().basic(bob, bob + "-secret")
                .accept(ksqlMediaType)
                .contentType(ksqlMediaType)
                .body(body2)
                .when()
                    .post("/ksql")
                .then()
                    .log().all()
                    .statusCode(200);


        Thread.sleep(1000); // wait for state store to be ready, TODO: check whether there's a smarter way to do this.

        final var pullQuery = Map.of("ksql", "SELECT * from aggs where product = 'milk';");

        given()
                .auth().preemptive().basic(bob, bob + "-secret")
                .accept(ksqlMediaType)
                .contentType(ksqlMediaType)
                .body(pullQuery)
                .when()
                    .post("/query")
                .then()
                    .log().all()
                    .statusCode(200)
                    .body("[1].row.columns[1]", is(18));
    }

}
