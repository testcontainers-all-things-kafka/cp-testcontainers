package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.DataGenConfig;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.restassured.RestAssured.given;

public class ConnectRbacTest {

    void setUpAccess(ConfluentServerContainer cpServer) {

        RestAssured.port = cpServer.getMdsPort();

        // alice is our trusted admin
        final var a = "alice";
        final var as = "alice-secret";


        final String srUser = "sr-user";
        // get Kafka cluster id
        final var id = given().
                when().
                get("/v1/metadata/id").
                then().
                statusCode(200).
                log().all().extract().body().path("id").toString();

        final var clusters = Map.of("clusters",
                Map.of("kafka-cluster", id,"connect-cluster", "connect"));

        // grant connect principal the security admin role on the connect cluster
        given().auth().preemptive().basic(a, as)
                .body(clusters)
                .contentType("application/json")
                .log().all()
                .when()
                .post("/security/1.0/principals/User:connect-principal/roles/SecurityAdmin")
                .then().log().all().statusCode(204);

        // add ClusterAdmin role so that we can view connector status
        given().auth().preemptive().basic(a, as)
                .body(clusters)
                .contentType("application/json")
                .when()
                .post("/security/1.0/principals/User:connect-principal/roles/SystemAdmin")
                .then().log().all().statusCode(204);

        // grant ResourceOwner on connect group and on the topics
        var connectGroup = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", id)),
                "resourcePatterns", List.of(
                        Map.of("resourceType", "Group","name", "connect","patternType", "LITERAL"),
                        Map.of("resourceType", "Topic","name", "connect-configs","patternType", "LITERAL"),
                        Map.of("resourceType", "Topic","name", "connect-offsets","patternType", "LITERAL"),
                        Map.of("resourceType", "Topic","name", "connect-status","patternType", "LITERAL"),
                        Map.of("resourceType", "Topic","name", "datagen","patternType", "LITERAL")
                ));

        given().auth().preemptive().basic(a, as)
                 .body(connectGroup)
                 .contentType("application/json")
                 .when()
                 .post("/security/1.0/principals/User:connect-principal/roles/ResourceOwner/bindings")
                 .then().log().all().statusCode(204);
    }

    @Test
    public void setupRbacConnectFailsWithoutAuthZ() throws ExecutionException, InterruptedException, IOException {
        final String connectPrincipal = "connect-principal";
        final String connectSecret = connectPrincipal + "-secret";
        final var factory = new CPTestContainerFactory();
        final var ldap = factory.createLdap(Set.of("alice", "bob", connectPrincipal));

        final var cServer = factory.createConfluentServer().enableRbac();
        final var connect = factory.createConfluentServerConnect(List.of("confluentinc/kafka-connect-datagen:0.4.0"), cServer)
                .enableRbac("http://kafka:8090", connectPrincipal, connectSecret)
                .withLogConsumer(o -> System.out.print(o.getUtf8String()));

        Startables.deepStart(Set.of(ldap, cServer)).get();
        setUpAccess(cServer);

        connect.start();

        RestAssured.port = connect.getMappedHttpPort();
        given().auth().preemptive().basic(connectPrincipal, connectSecret)
                .when()
                .get("/connectors")
                .then().log().all().statusCode(200);

        ConnectClient client = new ConnectClient(connect.getBaseUrl(), connectPrincipal, connectSecret);

        final var dataGenConfig = new DataGenConfig("datagen-connector")
                .withKafkaTopic("datagen")
                .withQuickstart("inventory")
                .withIterations(1000)
                .with("value.converter.schemas.enable", "false");
        client.startConnector(dataGenConfig);

        RestAssured.port = connect.getMappedHttpPort();
        given().auth().preemptive().basic(connectPrincipal, connectSecret)
                .when()
                .get("/connectors")
                .then().log().all().statusCode(200);

    }
}
