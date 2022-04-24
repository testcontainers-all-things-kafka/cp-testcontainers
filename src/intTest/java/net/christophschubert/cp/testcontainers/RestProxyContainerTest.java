package net.christophschubert.cp.testcontainers;

import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import io.restassured.RestAssured;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RestProxyContainerTest {

    final String restProxyFormatV2Json = "application/vnd.kafka.json.v2+json";
    final String restProxyFormatV2Avro = "application/vnd.kafka.avro.v2+json";
    @Test
    public void setupRestProxy() {
        final var containerFactory = new CPTestContainerFactory();

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var restProxy = containerFactory.createRestProxy(kafka);
        restProxy.start();

        RestAssured.port = restProxy.getMappedPort(RestProxyContainer.defaultPort);


        given()
                .when()
                .get("/topics")
                .then()
                .statusCode(200)
                .body("", hasSize(0));
    }

    @Test
    public void produceAndConsumer() throws InterruptedException {
        final var containerFactory = new CPTestContainerFactory();

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var restProxy = containerFactory.createRestProxy(kafka);
        restProxy.start();

        RestAssured.port = restProxy.getMappedPort(RestProxyContainer.defaultPort);

        final String topicName = "test_topic";

        var data = Map.of("records", List.of(
                Map.of("key", "key-1", "value", "value-1"),
                Map.of("key", "key-2", "value", "value-2")));

        //create consumer instance
        final var instanceName = "consumer-1";
        final var groupId = "group-1";
        given()
                .contentType(restProxyFormatV2Json)
                .body(Map.of("name", instanceName, "format", "json", "auto.offset.reset", "earliest", "fetch.min.bytes", "1"))
                .when()
                .post("/consumers/" + groupId)
                .then()
                .statusCode(200).log();

        final var consumerBaseUri = "/consumers/group-1/instances/consumer-1";
        // TODO: consider whether REST proxy should export external URL as advertised listener
        //subscribe to topic
        given()
               .contentType("application/vnd.kafka.v2+json")
                .body(Map.of("topics", List.of(topicName)))
                .when()
                .post(consumerBaseUri + "/subscription")
                .then()
                .statusCode(204).log();

        given()
                .contentType(restProxyFormatV2Json)
                .body(data)
                .when()
                .post("/topics/" + topicName)
                .then()
                .statusCode(200)
                .body("offsets", hasSize(2))
                .body("offsets[0].error", is(nullValue()))
                .body("offsets[1].error", is(nullValue()));

        given()
                .accept(restProxyFormatV2Json)
                .when()
                .get(consumerBaseUri + "/records")
                .then()
                .statusCode(200)
                .body("", hasSize(2))
                .body("[0].topic", is(topicName))
                .body("[0].key", is("key-1"));

    }

    @Test
    public void setupRestProxyWithSchemaRegistry() throws ExecutionException, InterruptedException {
        final var containerFactory = new CPTestContainerFactory();

        final var kafka = containerFactory.createKafka();
        final var schemaRegistry = containerFactory.createSchemaRegistry(kafka);
        final var restProxy = containerFactory.createRestProxy(kafka).withSchemaRegistry(schemaRegistry);
        Startables.deepStart(List.of(kafka, schemaRegistry, restProxy)).get();

        RestAssured.port = restProxy.getMappedPort(RestProxyContainer.defaultPort);

        final var schema = "{\n" +
                "     \"type\": \"record\",\n" +
                "     \"name\": \"User\",\n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"id\", \"type\": \"long\" },\n" +
                "       { \"name\": \"name\", \"type\": \"string\" }\n" +
                "     ]\n" +
                "}";

        final String topicName = "test_topic";

        var data = Map.of(
                "value_schema", schema,
                "records", List.of(
                    Map.of( "value", Map.of("id", 1, "name", "alice")),
                    Map.of( "value", Map.of("id", 2, "name", "barnie"))
        ));

        given()
                .contentType(restProxyFormatV2Avro)
                .body(data)
//                .log().all()
                .when()
                .post("/topics/" + topicName)
                .then()
                .statusCode(200)
//                .log().body()
                .body("offsets", hasSize(2))
                .body("offsets[0].error", is(nullValue()))
                .body("offsets[1].error", is(nullValue()));
    }
}
