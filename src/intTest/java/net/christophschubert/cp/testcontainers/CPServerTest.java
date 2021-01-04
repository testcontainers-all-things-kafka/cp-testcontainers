package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.TestClients;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

import static io.restassured.RestAssured.given;

public class CPServerTest {
    @Test
    public void createCPServerTest() {
        final var factory = new CPTestContainerFactory();
        final var cpServer = factory.createCPServer();

        cpServer.start();
        RestAssured.port = cpServer.getMappedPort(8090);

        // MDS API should return 404 as no MDS is configured by default.
        given().when().get("/security/1.0/features").then().statusCode(404).log().all();

        TestClients.basicReadWriteTest(cpServer.getBootstrapServers());
        //TODO: even this basic test generates a ton of error messages since the _confluent_telemetry_enabled topic is created
        // with replication.factor=3. Should look into this!

    }

    @Test
    public void startCPServerWithMdsLdap() {
        final var factory = new CPTestContainerFactory();
        final var ldap = factory.createLdap();
        ldap.start();

        final var cpServer = factory.createCPServer();
        factory.configureContainerForRBAC(cpServer);
//        cpServer.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        cpServer.start();

        RestAssured.port = cpServer.getMappedPort(8090);

        given().when().get("/security/1.0/features").then().log().all();
    }
}
