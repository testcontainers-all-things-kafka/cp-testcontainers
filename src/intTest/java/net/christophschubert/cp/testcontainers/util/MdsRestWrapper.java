package net.christophschubert.cp.testcontainers.util;

import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;

/**
 * Helper class to perform role bindings against MDS server listening on localhost.
 *
 * All methods contain assertions to check against HTTP status codes indicating success.
 */
public class MdsRestWrapper {

    public enum ClusterRole {
        SystemAdmin("SystemAdmin"),
        ClusterAdmin("ClusterAdmin"),
        UserAdmin("UserAdmin"),
        SecurityAdmin("SecurityAdmin"),
        AuditAdmin("AuditAdmin"),
        Operator("Operator");

        public final String roleName;

        ClusterRole(String roleName) {
            this.roleName = roleName;
        }
    }

    public enum ResourceRole {
        DeveloperManage("DeveloperManage"),
        DeveloperRead("DeveloperRead"),
        DeveloperWrite("DeveloperWrite"),
        ResourceOwner("ResourceOwner");

        public final String roleName;

        ResourceRole(String roleName) {
            this.roleName = roleName;
        }
    }

    public enum ClusterType {

        ConnectCluster("connect-cluster"),
        KsqlCluster("ksql-cluster"),
        SchemaRegistryCluster("schema-registry-cluster");

        final String clusterType;

        ClusterType(String clusterType) {
            this.clusterType = clusterType;
        }
    }

    public enum KafkaResourceType {
        Cluster("Cluster"),
        Group("Group"),
        Topic("Topic"),
        TransactionalId("TransactionalId");

        final String resourceType;

        KafkaResourceType(String resourceType) {
            this.resourceType = resourceType;
        }
    }

    public enum ResourceType {
        Connector("Connector"),
        KsqlCluster("KsqlCluster"), //TODO: deprecate
        KsqlClusterResource("KsqlCluster"), // to increase readability
        Subject("Subject");

        final String resourceType;
        ResourceType(String resourceType) {
            this.resourceType = resourceType;
        }
    }


    String kafkaClusterId;
    int port;
    String mdsUser;
    String mdsUserPassword;

    public MdsRestWrapper(int port, String mdsUser, String mdsUserPassword) {
        this.port = port;
        kafkaClusterId = getKafkaClusterId();
        this.mdsUser = mdsUser;
        this.mdsUserPassword = mdsUserPassword;
    }

    public String getKafkaClusterId() {
        return given().
                when().
                    port(port).
                    get("/v1/metadata/id").
                then().
                    statusCode(200).
                    log().all().extract().body().path("id").toString();
    }


    public void grantRoleOnCluster(String principal, ClusterRole role, ClusterType clusterType, String clusterName) {
        grantRoleOnCluster(principal, role.roleName, clusterType.clusterType, clusterName);
    }

    /**
     * ClusterType can be any of:
     * "schema-registry-cluster", "ksql-cluster"
     *
     * @param principal
     * @param role
     * @param clusterType
     * @param clusterName
     */
    @Deprecated
    public void grantRoleOnCluster(String principal, String role, String clusterType, String clusterName) {
        final var clusters = Map.of("clusters", Map.of("kafka-cluster", kafkaClusterId, clusterType, clusterName));

        given().auth().preemptive().basic(mdsUser, mdsUserPassword).
                accept("application/json").
                contentType("application/json").
                port(port).
                body(clusters).
                when().
                post("/security/1.0/principals/User:" + principal + "/roles/" + role).
                then().log().all().statusCode(204);
    }

    public void grantRoleOnKafkaCluster(String principal, ClusterRole role) {
        final var clusters = Map.of("clusters", Map.of("kafka-cluster", kafkaClusterId));

        given().auth().preemptive().basic(mdsUser, mdsUserPassword).
                accept("application/json").
                contentType("application/json").
                port(port).
                body(clusters).
                when().
                post("/security/1.0/principals/User:" + principal + "/roles/" + role.roleName).
                then().log().all().statusCode(204);
    }


    public void grantRoleOnKafkaResource(String principal, ResourceRole role, KafkaResourceType resourceType, String resourceName) {
        grantRoleOnKafkaResource(principal, role, resourceType, resourceName, false);
    }

    // TODO: remove duplicate code here
    public void grantRoleOnResource(String principal, ResourceRole role, ClusterType clusterType, String clusterId, ResourceType resourceType, String resourceName, boolean isPrefixed) {
        final var patternType = isPrefixed ? "PREFIXED" : "LITERAL";
        var res = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", kafkaClusterId, clusterType.clusterType, clusterId)),
                "resourcePatterns", List.of(
                        Map.of("resourceType", resourceType.resourceType, "name", resourceName, "patternType", patternType)
                ));

        final var path = String.format("/security/1.0/principals/User:%s/roles/%s/bindings", principal, role.roleName);
        given()
                .auth().preemptive().basic(mdsUser, mdsUserPassword)
                .body(res)
                .contentType("application/json")
                .port(port)
        .when()
                .post(path)
        .then()
                .log().all()
                .statusCode(204);
    }

    public void grantRoleOnResource(String principal, ResourceRole role, ClusterType clusterType, String clusterId, ResourceType resourceType, String resourceName) {
        grantRoleOnResource(principal, role, clusterType, clusterId, resourceType, resourceName, false);
    }
    public void grantRoleOnKafkaResource(String principal, ResourceRole role, KafkaResourceType resourceType, String resourceName, boolean isPrefixed) {
        final var patternType = isPrefixed ? "PREFIXED" : "LITERAL";
        var res = Map.of("scope", Map.of("clusters", Map.of("kafka-cluster", kafkaClusterId)),
                "resourcePatterns", List.of(
                        Map.of("resourceType", resourceType.resourceType, "name", resourceName, "patternType", patternType)
                ));

        final var path = String.format("/security/1.0/principals/User:%s/roles/%s/bindings", principal, role.roleName);
        given().auth().preemptive().basic(mdsUser, mdsUserPassword)
                .body(res)
                .contentType("application/json")
                .port(port)
                .when()
                .post(path)
                .then().log().all().statusCode(204);
    }
}
