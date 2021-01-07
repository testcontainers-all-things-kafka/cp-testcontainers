package net.christophschubert.cp.testcontainers;

import io.restassured.RestAssured;
import net.christophschubert.cp.testcontainers.util.MdsRestWrapper;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.restassured.RestAssured.given;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ClusterType.KsqlCluster;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.KafkaResourceType.Group;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.KafkaResourceType.Topic;
import static net.christophschubert.cp.testcontainers.util.MdsRestWrapper.ResourceRole.ResourceOwner;
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

        // alice is our trusted admin
        final var a = "alice";
        final var as = "alice-secret";
        RestAssured.port = kafka.getMdsPort();

        final var mdsWrapper = new MdsRestWrapper(kafka.getMdsPort(), a, as);
        final var id = mdsWrapper.getKafkaClusterId();
        System.out.println(id);


        mdsWrapper.grantRoleOnCluster("ksql",
                MdsRestWrapper.ClusterRole.SecurityAdmin,
                KsqlCluster,
                "ksql");

        mdsWrapper.grantRoleOnResource("ksql", ResourceOwner, KsqlCluster, "ksql-cluster", MdsRestWrapper.ResourceType.KsqlCluster, "ksql-cluster");
        // this call above replaces the following
        //       final var clusters = Map.of("clusters",
        //                Map.of("kafka-cluster", id,"ksql-cluster", "ksql"));
        //        var scope1 = Map.of("scope", clusters,
//                "resourcePatterns", List.of(
//                        Map.of("resourceType", "KsqlCluster", "name", "ksql-cluster")
//                ));
//        given().auth().preemptive().basic(a, as)
//                .body(scope1)
//                .contentType("application/json")
//                .when()
//                .post("/security/1.0/principals/User:ksql/roles/ResourceOwner/bindings")
//                .then().log().all().statusCode(204);
//

        // it seems to me that the REST calls on https://docs.confluent.io/platform/current/security/rbac/rbac-config-using-rest-api.html
        // are not correct for ksql

        mdsWrapper.grantRoleOnKafkaResource("ksql", ResourceOwner, Group, "_confluent-ksql-ksql", true);
        mdsWrapper.grantRoleOnKafkaResource("ksql", ResourceOwner, Topic, "_confluent-ksql-ksql", true);
        mdsWrapper.grantRoleOnKafkaResource("ksql", ResourceOwner, Topic, "ksqlksql_processing_log");
        mdsWrapper.grantRoleOnKafkaResource("ksql", ResourceOwner, Topic, "ksql", true); // should change this

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
