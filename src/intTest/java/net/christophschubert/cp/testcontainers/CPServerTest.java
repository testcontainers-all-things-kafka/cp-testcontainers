package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.junit.Test;
import org.testcontainers.containers.Network;

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
}
