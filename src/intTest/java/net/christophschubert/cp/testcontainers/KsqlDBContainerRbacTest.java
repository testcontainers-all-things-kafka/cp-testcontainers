package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;

import java.util.List;
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
        ksqlDB.prettyPrintEnvs();
       // ksqlDB.start(); fails with KsqlTopicAuthorizationException (as it should, since no access has been configured.
//
//        RestAssured.port = ksqlDB.getFirstMappedPort();
//
//        given()
//                .when()
//                .get("/healthcheck")
//                .then()
//                .log().all()
//                .statusCode(200)
//                .body("isHealthy", is(true));
    }
}
