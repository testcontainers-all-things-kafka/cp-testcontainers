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
        final var cpServer = factory.createCPServer();

        cpServer.start();
        RestAssured.port = cpServer.getMappedPort(8090);

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
    public void startCPServerWithMdsLdap() {
        final Network network = Network.newNetwork();
        final var factory = new CPTestContainerFactory(network);
        final var rbacFactory = new RbacEnabledContainerFactory(network);
        final var ldap = rbacFactory.createLdap();
        ldap.start();

        final var cpServer = factory.createCPServer();
        rbacFactory.configureContainerForRBAC(cpServer);
        cpServer.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        cpServer.start();

        RestAssured.port = cpServer.getMappedPort(8090);

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
        final Network network = Network.newNetwork();
        final var factory = new CPTestContainerFactory(network);
        final var rbacFactory = new RbacEnabledContainerFactory(network);
        final var ldap = rbacFactory.createLdap();

        final var cpServer = factory.createCPServer();
        rbacFactory.configureContainerForRBAC(cpServer);
//        cpServer.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        Startables.deepStart(List.of(cpServer, ldap)).get();

        RestAssured.port = cpServer.getMappedPort(8090);

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

        final var sr = factory.createSchemaRegistry(cpServer).withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        rbacFactory.configureContainerForRBAC(sr);
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
        final var rbacFactory = new RbacEnabledContainerFactory(network);
        final var ldap = rbacFactory.createLdap();

        final var cpServer = factory.createCPServer();
        rbacFactory.configureContainerForRBAC(cpServer);

        final var sr = factory.createSchemaRegistry(cpServer);
        rbacFactory.configureContainerForRBAC(sr, "alice", "alice-secret");
        Startables.deepStart(List.of(cpServer, ldap, sr)).get();

        RestAssured.port = sr.getMappedHttpPort();

        given().when().get("subjects").then().statusCode(401);

        given().auth().preemptive().basic("alice", "alice-secret").when().get("subjects").then().statusCode(200).
                body("", is(Collections.emptyList())).log().all();
    }
}
