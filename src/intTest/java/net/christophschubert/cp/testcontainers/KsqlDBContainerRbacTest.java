package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.MdsRestWrappers;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

public class KsqlDBContainerRbacTest {

    @Test
    public void setupKsqlDB() throws ExecutionException, InterruptedException {
        final var containerFactory = new CPTestContainerFactory();
        final var ldap = containerFactory.createLdap(Set.of("alice", "bob", "ksql"));
        // users: alice is our trusted super-user
        // bob: wants to start interactive queries
        // ksql is the technical user for the ksqlDB cluster
        final var kafka = containerFactory.createConfluentServer().enableRbac();
        Startables.deepStart(Set.of(ldap, kafka)).get();

        final var ksqlDB = containerFactory.createKsqlDB(kafka).enableRbac("http://kafka:8090", "ksql", "ksql-secret");
        ksqlDB.withServiceId("ksql");
        ksqlDB.withStartupTimeout(Duration.ofMinutes(1));
//        ksqlDB.prettyPrintEnvs();

        // configure role-bindings
        // alice is our trusted user

        RestAssured.port = kafka.getMdsPort();

        final var id = MdsRestWrappers.getKafkaClusterId();
        System.out.println(id);

        // alice is our trusted admin
        final var a = "alice";
        final var as = "alice-secret";

        final var clusters = Map.of("clusters",
                Map.of("kafka-cluster", id,"ksql-cluster", "ksql"));

        // grant connect principal the security admin role on the connect cluster
        given().auth().preemptive().basic(a, as)
                .body(clusters)
                .contentType("application/json")
                .log().all()
                .when()
                .post("/security/1.0/principals/User:ksql/roles/SecurityAdmin")
                .then().log().all().statusCode(204);

        var scope1 = Map.of("scope", clusters,
                "resourcePatterns", List.of(
                        Map.of("resourceType", "KsqlCluster", "name", "ksql-cluster")
                ));


        given().auth().preemptive().basic(a, as)
                .body(scope1)
                .contentType("application/json")
                .when()
                .post("/security/1.0/principals/User:ksql/roles/ResourceOwner/bindings")
                .then().log().all().statusCode(204);


        // it seems to me that the REST calls on https://docs.confluent.io/platform/current/security/rbac/rbac-config-using-rest-api.html
        // are not correct for ksql
        var res = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", id)),
                "resourcePatterns", List.of(
                        Map.of("resourceType", "Group","name", "_confluent-ksql-ksql","patternType", "PREFIXED"),
                        Map.of("resourceType", "Topic","name", "_confluent-ksql-ksql","patternType", "PREFIXED"),
                        Map.of("resourceType", "Topic","name", "ksqlksql_processing_log","patternType", "LITERAL"),
                        Map.of("resourceType", "Topic","name", "ksql","patternType", "PREFIXED")
//                        Map.of("resourceType", "Topic","name", "datagen","patternType", "LITERAL")
                ));

        given().auth().preemptive().basic(a, as)
                .body(res)
                .contentType("application/json")
                .when()
                .post("/security/1.0/principals/User:ksql/roles/ResourceOwner/bindings")
                .then().log().all().statusCode(204);


        ksqlDB.start();


        RestAssured.port = ksqlDB.getFirstMappedPort();

        given().auth().preemptive().basic("ksql", "ksql-secret")
                .when()
                .get("/healthcheck")
                .then()
                .log().all()
                .statusCode(200)
                .body("isHealthy", is(true));
    }
}
