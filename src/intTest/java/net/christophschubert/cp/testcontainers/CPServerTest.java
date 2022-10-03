package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.MdsRestWrapper;
import net.christophschubert.cp.testcontainers.util.TestClients;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.restassured.RestAssured;

import static io.restassured.RestAssured.given;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ClusterRole.SecurityAdmin;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.KafkaResourceType.Group;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.KafkaResourceType.Topic;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ResourceRole.DeveloperRead;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ResourceRole.DeveloperWrite;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ResourceRole.ResourceOwner;
import static net.christophschubert.cp.testcontainers.util.TestContainerUtils.startAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

public class CPServerTest {
    // our super.user
    static final String alice = "alice";
    static final String aliceSecret = "alice-secret";

    @Test
    public void createCPServerTest() {
        final var factory = new CPTestContainerFactory();
        try (var cpServer = factory.createConfluentServer()) {

            cpServer.start();
            RestAssured.port = cpServer.getMdsPort();

            // Even without RBAC enabled we should be able to get the Kafka cluster ID as this is part of the REST proxy
            // embedded in Confluent Server.
            // See https://docs.confluent.io/platform/current/kafka-rest/api.html#crest-api-v3 for documentation on the API.
            // TODO: question: how is this REST interface secured?
            given().
                    when().
                    get("/v1/metadata/id").
                    peek().
                    then().
                    statusCode(200).
                    body("id", is(notNullValue()));

            // REST proxy is enabled by default
            given().
                    when().
                    get("/kafka/v3/clusters").
                    then().
                    statusCode(200).
                    log().all();

            // MDS API should return 404 as no security is configured by default.
            given()
                    .when()
                    .get("/security/1.0/features")
                    .then()
                    .statusCode(404);

            TestClients.basicReadWriteTest(cpServer.getBootstrapServers());
        }
        //TODO: even this basic test generates a ton of error messages since the _confluent_telemetry_enabled topic is created
        // with replication.factor=3. Should look into this!
    }

    @Test
    @Disabled
    public void startCPServerWithMdsLdap() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final ConfluentServerContainer cpServer;
        try (var ldap = factory.createLdap()) {

            cpServer = factory.createConfluentServer().enableRbac();

            startAll(ldap, cpServer);
        }

        RestAssured.port = cpServer.getMdsPort();

        given().
                when().
                get("/security/1.0/features").
                then().
                statusCode(200).
                body("features.'basic.auth.1.enabled'", is(true));

        given().
                when().
                get("/v1/metadata/id").
                then().
                log().all().
                statusCode(200);

//        //TODO: embedded REST proxy gives 500, shouldn't it rather be a 401?
//        given().
//                get("/kafka/v3/clusters").
//                then().
//                statusCode(500).
//                log().all();

        // TODO: even authenticating as a super.user gives 500,
        // how can we actually authenticate when RBAC is enabled?
//        given().
//                auth().preemptive().basic("alice", "alice-secret").
//                get("/kafka/v3/clusters").
//                then().
//                statusCode(500).
//                log().all();

        given().
                auth().preemptive().basic("alice", "alice-secret").
                accept("application/json").
                when().
                get("/security/1.0/authenticate").
                then().
                statusCode(200).
                body("auth_token", is(notNullValue())).
                log().all();
        given().
                auth().preemptive().basic("mds", "mds-secret").
                accept("application/json").
                when().
                get("/security/1.0/authenticate").
                then().
                statusCode(401);
    }

    @Test
    @Disabled
    public void startRbacSchemaRegistry() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final ConfluentServerContainer cpServer;
        try (var ldap = factory.createLdap()) {
            cpServer = factory.createConfluentServer().enableRbac();
            startAll(cpServer, ldap);
        }

        final MdsRestWrapper mdsWrapper = new MdsRestWrapper(cpServer.getMdsPort(), alice, aliceSecret);

        final String srUser = "sr-user";

        try (var sr = factory.createSchemaRegistry(cpServer)
            .enableRbac()
            .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))) {

            mdsWrapper.grantRoleOnCluster(srUser, SecurityAdmin, MdsRestWrapper.ClusterType.SchemaRegistryCluster, sr.getClusterId());

            mdsWrapper.grantRoleOnKafkaResource(srUser, ResourceOwner, Topic, sr.getSchemasTopic());
            mdsWrapper.grantRoleOnKafkaResource(srUser, ResourceOwner, Group, sr.getClusterId());

            for (var role : List.of(DeveloperRead, DeveloperWrite)) {
                mdsWrapper.grantRoleOnKafkaResource(srUser, role, Topic, cpServer.licenseTopic());
            }

            //TODO:
            //it seems that the documentation at https://docs.confluent.io/platform/current/security/rbac/rbac-config-using-rest-api.html
            // has a mistake: rights for topics are not mentioned
            sr.start();

            RestAssured.port = sr.getMappedHttpPort();
        }
        given().when().get("subjects").then().statusCode(401);
        given().auth().preemptive().basic(srUser, "wrong-password").when().get("subjects").then().statusCode(401);
        given().auth().preemptive().basic(srUser, "sr-user-secret").when().get("subjects").then().statusCode(200).
                body("", is(Collections.emptyList())).log().all();


    }

    @Test
    @Disabled
    public void superUserShouldStartSRWithoutBinding() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final var ldap = factory.createLdap();

        final var cpServer = factory.createConfluentServer().enableRbac();

//        final var mdsBootstrap = String.format("http://%s:8090", cpServer.getNetworkAliases().get(0));
        final var sr = factory
                .createSchemaRegistry(cpServer)
                .enableRbac(cpServer.getMdsUrl(), "alice", "alice-secret"); //alice is an implicit super-user in the Kafka cluster
        startAll(cpServer, ldap, sr);


        final String srUser = "sr-user";

        final MdsRestWrapper mdsWrapper = new MdsRestWrapper(cpServer.getMdsPort(), alice, aliceSecret);

        mdsWrapper.grantRoleOnCluster(srUser, SecurityAdmin, MdsRestWrapper.ClusterType.SchemaRegistryCluster, sr.getClusterId());

        mdsWrapper.grantRoleOnKafkaResource(srUser, ResourceOwner, Group, sr.getClusterId());
        mdsWrapper.grantRoleOnKafkaResource(srUser, ResourceOwner, Topic, sr.getSchemasTopic());

        for (var role : List.of(DeveloperRead, DeveloperWrite)) {
            mdsWrapper.grantRoleOnKafkaResource(srUser, role, Topic, cpServer.licenseTopic());
        }

        // alice is Kafka super-user, which does not help her when registering schemas (?!), let's give her the rights.
        for (var role : List.of(DeveloperRead, DeveloperWrite)) {
            mdsWrapper.grantRoleOnResource(alice, role, MdsRestWrapper.ClusterType.SchemaRegistryCluster, sr.getClusterId(), MdsRestWrapper.ResourceType.Subject, "test");

        }


        RestAssured.port = sr.getMappedHttpPort();
        given().when().get("subjects").then().statusCode(401);
        given().auth().preemptive().basic("alice", "alice-secret").contentType("application/vnd.schemaregistry.v1+json").when().get("subjects").then().statusCode(200).
                body("", is(Collections.emptyList())).log().all();

        final var st = "{\"schema\": \"{ \\\"type\\\": \\\"record\\\", \\\"name\\\": \\\"test\\\", \\\"fields\\\": [ { \\\"type\\\": \\\"string\\\", \\\"name\\\": \\\"field1\\\" }, { \\\"type\\\": \\\"int\\\", \\\"name\\\": \\\"field2\\\" } ] }\" }";
        System.out.println(st);
        given().auth().preemptive().basic("alice", "alice-secret").contentType("application/vnd.schemaregistry.v1+json").body(st).when().post("subjects/test/versions").then().log().all().statusCode(200).body("id", is(1));
    }

    // after enableRbac is called, clients should be able to authenticate towards Kafka with their LDAP credentials
    @Test
    @Disabled
    public void superUserCanProducerUsingSaslPlain() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final var ldap = factory.createLdap(Set.of("alice", "mds", "producer", "consumer"));

        final var cpServer = factory.createConfluentServer().enableRbac();

        startAll(cpServer, ldap);

        final var topicName = "testTopic";

        final var producer = TestClients.createProducer(cpServer.getBootstrapServers(), SecurityConfigs.plainJaasProperties("alice", "alice-secret"));
        final var recordMetadata = producer.send(new ProducerRecord<>(topicName, "hello-world")).get();
        assertThat(0L).isEqualTo(recordMetadata.offset());
        System.out.println(recordMetadata);
        final var producerNonAuth = TestClients.createProducer(cpServer.getBootstrapServers(), SecurityConfigs.plainJaasProperties("alice", "alice-wrongpassword"));
        try {
            producerNonAuth.send(new ProducerRecord<>(topicName, "hello-world")).get();
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(SaslAuthenticationException.class);
            producer.close();
            return;
        }
        fail(null);
    }

    @Test
    @Disabled
    public void ldapAuthenticationWorksForClients() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final var ldap = factory.createLdap(Set.of("alice", "mds", "producer", "consumer"));

        final String topicName;
        final org.apache.kafka.clients.producer.Producer<String, String> producer;
        MdsRestWrapper mdsWrapper;
        try (var cpServer = factory.createConfluentServer().enableRbac()) {
            startAll(cpServer, ldap);
            topicName = "testTopic";

            producer = TestClients.createProducer(cpServer.getBootstrapServers(), SecurityConfigs.plainJaasProperties("producer", "producer-secret"));

            try {
                producer.send(new ProducerRecord<>(topicName, "never going to be produced value")).get();
                fail(null); // fail if no exception was thrown
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(TopicAuthorizationException.class);
                producer.close();
            }

            mdsWrapper = new MdsRestWrapper(cpServer.getMdsPort(), "alice", "alice-secret");
        }
        //should grant resource-owner as principal need rights to create previously non-existing topic
        mdsWrapper.grantRoleOnKafkaResource("producer", ResourceOwner, Topic, topicName);

        final var recordMetadata = producer.send(new ProducerRecord<>(topicName, "hello-world")).get();
        Assertions.assertThat(0L).isEqualTo(recordMetadata.offset());
        System.out.println(recordMetadata);
    }


    @Test
    @Disabled
    public void clientLdapAuthenticationWorksWithSchemaRegistry() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final var ldap = factory.createLdap(Set.of("alice", "mds", "sr-user", "producer", "consumer"));

        final var cpServer = factory.createConfluentServer().enableRbac();
        final var schemaRegistry = factory.createSchemaRegistry(cpServer).enableRbac();

        startAll(ldap, cpServer);
        var mdsWrapper = new MdsRestWrapper(cpServer.getMdsPort(), "alice", "alice-secret");

        final var topicName = "testTopic";

        final var srPrincipal = "sr-user";

        mdsWrapper.grantRoleOnCluster(srPrincipal, SecurityAdmin, MdsRestWrapper.ClusterType.SchemaRegistryCluster, schemaRegistry.getClusterId());
        mdsWrapper.grantRoleOnKafkaResource(srPrincipal, ResourceOwner, Topic, schemaRegistry.getSchemasTopic());
        mdsWrapper.grantRoleOnKafkaResource(srPrincipal, ResourceOwner, Group, schemaRegistry.getClusterId());
        for (var role : List.of(DeveloperRead, DeveloperWrite)) {
            mdsWrapper.grantRoleOnKafkaResource(srPrincipal, role, Topic, cpServer.licenseTopic());
        }

        schemaRegistry.start();

        final Schema s = SchemaBuilder.builder().record("User").fields().requiredString("email").requiredInt("age").endRecord();
        final var originalRecord = new GenericRecordBuilder(s).set("email", "peter@a.com").set("age", 18).build();

        final Map<String, Object> addProps = new HashMap<>(SecurityConfigs.plainJaasProperties("producer", "producer-secret"));
        // TODO: finding the proper setting wasn't easy, maybe documentation should be improved:
        addProps.put("basic.auth.credentials.source", "USER_INFO");
        addProps.put("schema.registry.basic.auth.user.info", "producer:producer-secret");

        final var producer = TestClients.createAvroProducer(cpServer.getBootstrapServers(),
                schemaRegistry.getBaseUrl(),
                addProps);


        //should grant resource-owner as principal need rights to create previously non-existing topic
        mdsWrapper.grantRoleOnKafkaResource("producer", ResourceOwner, Topic, topicName);

        try {
            producer.send(new ProducerRecord<>(topicName, originalRecord)).get();
            fail(null); // exception should have been thrown
        } catch (InterruptedException | ExecutionException | SerializationException e) {
            if (! (e.getCause() instanceof RestClientException))
                fail("expecting RestClientException");
            final RestClientException re = (RestClientException) e.getCause();
            System.out.println(e.getCause());
            assertThat(re.getErrorCode()).isEqualTo(40301);
        }
        mdsWrapper.grantRoleOnResource("producer", DeveloperWrite, MdsRestWrapper.ClusterType.SchemaRegistryCluster,
                schemaRegistry.getClusterId(), MdsRestWrapper.ResourceType.Subject, topicName + "-value");

        final var recordMetadata = producer.send(new ProducerRecord<>(topicName, originalRecord)).get();
        assertThat(recordMetadata.offset()).isEqualTo(0);
    }
}
