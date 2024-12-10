package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.Tag;

import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.stream.Stream;

import io.restassured.RestAssured;

import static io.restassured.RestAssured.given;
import static net.christophschubert.cp.testcontainers.util.TestContainerUtils.startAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

/**
 * Demonstrates the use of withTag to set the CP version we want to start.
 * <p>
 * This test will pull a lot of different Docker image versions.
 */
public class MultiVersionTest {
  

  private static Stream<Arguments> getCpVersions() {
    // Test against current supported versions of Confluent Platform
    return Stream.of(
        Arguments.of("7.5.2"),  // Previous stable version
        Arguments.of("7.6.0"),  // Previous stable version
        Arguments.of("7.7.0"),  // Previous stable version
        Arguments.of("7.8.0")   // Latest stable version
    );
  }

  @ParameterizedTest(name = "should start Apache Kafka v{0}")
  @MethodSource("getCpVersions")
  public void vanillaKafka(String tag) {
    final var factory = new CPTestContainerFactory().withTag(tag);
    final var kafka = factory.createKafka();
    kafka.start();
    
    // Wait for logs to accumulate and verify startup
    Awaitility.await()
        .atMost(Durations.FIVE_MINUTES)  // Increased timeout for ARM emulation
        .pollInterval(Durations.TEN_SECONDS)
        .untilAsserted(() -> 
            assertThat(kafka.getLogs())
                .contains("INFO [KafkaServer id=1] started (kafka.server.KafkaServer)"));
  }

  @ParameterizedTest(name = "should start Confluent Server v{0}")
  @MethodSource("getCpVersions")
  public void server(String tag) {
    final var factory = new CPTestContainerFactory().withTag(tag);
    final var cpServer = factory.createConfluentServer();
    cpServer.start();

    // Wait for logs to accumulate and verify startup
    Awaitility.await()
        .atMost(Durations.FIVE_MINUTES)  // Increased timeout for ARM emulation
        .pollInterval(Durations.TEN_SECONDS)
        .untilAsserted(() -> {
            final var logs = cpServer.getLogs();
            assertThat(logs).contains(String.format("INFO Kafka version: %s-ce", tag));
            assertThat(logs).contains("INFO Kafka startTimeMs:");
            assertThat(logs).contains("INFO [KafkaServer id=1] started (kafka.server.KafkaServer)");
        });

    RestAssured.port = cpServer.getMdsPort();

    Awaitility.await()
        .atMost(Durations.TWO_MINUTES)  // Increased timeout for ARM emulation
        .pollInterval(Durations.TEN_SECONDS)
        .untilAsserted(() ->
            given()
                .when()
                .get("/v1/metadata/id")
                .then()
                .statusCode(200)
                .body("id", is(notNullValue())));
  }


  @ParameterizedTest(name = "should start container (with Kafka v{0})")
  @MethodSource("getCpVersions")
  public void serverWithRbac(String tag) throws InterruptedException {
    final var featuresEndpointAddedTag = new Tag("6"); // '/security/1.0/features' endpoint was added in CP 6.0.0

    final var factory = new CPTestContainerFactory().withTag(tag);
    final var ldap = factory.createLdap();
    final var cpServer = factory.createConfluentServer().enableRbac();
    startAll(ldap, cpServer);

    final var logs = cpServer.getLogs();
    assertThat(logs.contains(String.format("INFO Kafka version: %s-ce", tag))).isTrue();
    assertThat(logs.contains("INFO Kafka startTimeMs:")).isTrue();
    assertThat(logs.contains("INFO [KafkaServer id=1] started (kafka.server.KafkaServer)")).isTrue();

    RestAssured.port = cpServer.getMdsPort();
    // Even without RBAC enabled, we should be able to get the Kafka cluster ID as this is part of the REST proxy
    // embedded in Confluent Server.
    given().
        when().
        get("/v1/metadata/id").
        then().
        statusCode(200).
        body("id", is(notNullValue(String.class)));

    if (new Tag(tag).atLeast(featuresEndpointAddedTag)) {
      //check whether RBAC is enabled
      given().
          when().
          get("/security/1.0/features").
          then().
          statusCode(200).
          body("features.'basic.auth.1.enabled'", is(true));
    }
  }
}
