package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

public class CPServerTest {
    @Test
    public void createCPServerTest() {
        final var factory = new CPTestContainerFactory();
        final var cpServer = factory.createConfluentServer();

        cpServer.start();
        RestAssured.port = cpServer.getMdsPort();

        given().
                when().
                    get("/v1/metadata/id").
                then().
                    statusCode(200).
                    body("id", is(notNullValue()));

        // MDS API should return 404 as no security is configured by default.
        given()
                .when()
                    .get("/security/1.0/features")
                .then()
                    .statusCode(404);

        TestClients.basicReadWriteTest(cpServer.getBootstrapServers());
        //TODO: even this basic test generates a ton of error messages since the _confluent_telemetry_enabled topic is created
        // with replication.factor=3. Should look into this!

    }

    @Test
    public void startCPServerWithMdsLdap() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final var ldap = factory.createLdap();

        final var cpServer = factory.createConfluentServer().enableRbac();
//        cpServer.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));

        Startables.deepStart(List.of(ldap, cpServer)).get();

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
                    statusCode(200).
                    log().all();
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
    public void startRbacSchemaRegistry() throws ExecutionException, InterruptedException {
        final var factory = new CPTestContainerFactory();
        final var ldap = factory.createLdap();

        final var cpServer = factory.createConfluentServer().enableRbac();

//        cpServer.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        Startables.deepStart(List.of(cpServer, ldap)).get();

        RestAssured.port = cpServer.getMdsPort();

        final String srUser = "sr-user";

        final var id = given().
                when().
                get("/v1/metadata/id").
                then().
                statusCode(200).
                log().all().extract().body().path("id").toString();

        final var clusters = Map.of("clusters", Map.of("kafka-cluster", id, "schema-registry-cluster", "schema-registry"));

        given().auth().preemptive().basic("alice", "alice-secret").
                accept("application/json").
                contentType("application/json").
                body(clusters).
                when().
                post("/security/1.0/principals/User:"+ srUser +"/roles/SecurityAdmin").
                then().statusCode(204).log().all();


        final var scope = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", id)),
                "resourcePatterns", List.of(Map.of("resourceType", "Group","name", "schema-registry","patternType", "LITERAL"),
                        Map.of("resourceType", "Topic","name", "_schemas","patternType", "LITERAL")));


        given().auth().preemptive().basic("alice", "alice-secret").
                accept("application/json").
                contentType("application/json").
                body(scope).
                post("/security/1.0/principals/User:" + srUser +"/roles/ResourceOwner/bindings").
                then().statusCode(204).log().all();

        final var licenseScope = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", id)),
                "resourcePatterns", List.of(Map.of("resourceType", "Topic","name", "_confluent-license","patternType", "LITERAL")
                       ));

        for (String role : List.of("DeveloperRead", "DeveloperWrite")) {
            given().auth().preemptive().basic("alice", "alice-secret").
                    accept("application/json").
                    contentType("application/json").
                    body(licenseScope).
                    post(String.format("/security/1.0/principals/User:%s/roles/%s/bindings", srUser, role)).
                    then().statusCode(204);
        }

        //TODO:
        //it seems that the documentation at https://docs.confluent.io/platform/current/security/rbac/rbac-config-using-rest-api.html
        // has a mistake: rights for topics are not mentioned

        final var sr = factory.createSchemaRegistry(cpServer)
                .enableRbac()
                .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));

        sr.start();

        RestAssured.port = sr.getMappedHttpPort();

        given().when().get("subjects").then().statusCode(401);
        given().auth().preemptive().basic(srUser, "wrong-password").when().get("subjects").then().statusCode(401);
        given().auth().preemptive().basic(srUser, "sr-user-secret").when().get("subjects").then().statusCode(200).
                body("", is(Collections.emptyList())).log().all();


    }

    @Test
    public void superUserShouldStartSRWithoutBinding() throws ExecutionException, InterruptedException {
        final Network network = Network.newNetwork();
        final var factory = new CPTestContainerFactory(network);
        final var ldap = factory.createLdap();

        final var cpServer = factory.createConfluentServer().enableRbac();

        final var sr = factory
                .createSchemaRegistry(cpServer)
                .enableRbac("http://kafka:8090", "alice", "alice-secret"); //alice is an implicit super-user in the Kafka cluster
        Startables.deepStart(List.of(cpServer, ldap, sr)).get();



        ///////////////


        RestAssured.port = cpServer.getMdsPort();

        final String srUser = "sr-user";

        final var id = given().
                when().
                get("/v1/metadata/id").
                then().
                statusCode(200).
                log().all().extract().body().path("id").toString();

        final var clusters = Map.of("clusters", Map.of("kafka-cluster", id, "schema-registry-cluster", "schema-registry"));

        given().auth().preemptive().basic("alice", "alice-secret").
                accept("application/json").
                contentType("application/json").
                body(clusters).
                when().
                post("/security/1.0/principals/User:"+ srUser +"/roles/SecurityAdmin").
                then().statusCode(204).log().all();


        final var scope = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", id)),
                "resourcePatterns", List.of(Map.of("resourceType", "Group","name", "schema-registry","patternType", "LITERAL"),
                        Map.of("resourceType", "Topic","name", "_schemas","patternType", "LITERAL")));


        given().auth().preemptive().basic("alice", "alice-secret").
                accept("application/json").
                contentType("application/json").
                body(scope).
                post("/security/1.0/principals/User:" + srUser +"/roles/ResourceOwner/bindings").
                then().statusCode(204).log().all();

        final var licenseScope = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", id)),
                "resourcePatterns", List.of(Map.of("resourceType", "Topic","name", "_confluent-license","patternType", "LITERAL")
                ));

        for (String role : List.of("DeveloperRead", "DeveloperWrite")) {
            given().auth().preemptive().basic("alice", "alice-secret").
                    accept("application/json").
                    contentType("application/json").
                    body(licenseScope).
                    post(String.format("/security/1.0/principals/User:%s/roles/%s/bindings", srUser, role)).
                    then().statusCode(204);
        }



        // alice is Kafka super-user, which does not help her when registering schemas (?!), let's give her the rights.
        final var schemasScope = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", id, "schema-registry-cluster", "schema-registry")),
                "resourcePatterns", List.of(Map.of("resourceType", "Subject","name", "test","patternType", "LITERAL")
                ));
        for (String role : List.of("DeveloperRead", "DeveloperWrite")) {
            given().auth().preemptive().basic("alice", "alice-secret").
                    accept("application/json").
                    contentType("application/json").
                    body(schemasScope).
                    post(String.format("/security/1.0/principals/User:%s/roles/%s/bindings", "alice", role)).
                    then().log().all();
        }

        ////////////////

        RestAssured.port = sr.getMappedHttpPort();

        given().when().get("subjects").then().statusCode(401);

        given().auth().preemptive().basic("alice", "alice-secret").contentType("application/vnd.schemaregistry.v1+json").when().get("subjects").then().statusCode(200).
                body("", is(Collections.emptyList())).log().all();

        final var st = "{\"schema\": \"{ \\\"type\\\": \\\"record\\\", \\\"name\\\": \\\"test\\\", \\\"fields\\\": [ { \\\"type\\\": \\\"string\\\", \\\"name\\\": \\\"field1\\\" }, { \\\"type\\\": \\\"int\\\", \\\"name\\\": \\\"field2\\\" } ] }\" }";
        System.out.println(st);
        given().auth().preemptive().basic("alice", "alice-secret").contentType("application/vnd.schemaregistry.v1+json").body(st).when().post("subjects/test/versions").then().log().all().statusCode(200).body("id", is(1));
    }


}
