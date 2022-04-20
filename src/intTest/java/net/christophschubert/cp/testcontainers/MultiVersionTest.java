package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.Tag;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
    // we test against the latest bug-fix version for each of the feature versions:
    return Stream.of(Arguments.of("5.4.6"), Arguments.of("5.5.7"), Arguments.of("6.0.5"), Arguments.of("6.1.4"), Arguments.of("6.2.2"), Arguments.of("7.0.0"), Arguments.of("7.0.1"));
  }

  @ParameterizedTest(name = "should start Apache Kafka v{0}")
  @MethodSource("getCpVersions")
  public void vanillaKafka(String tag) throws InterruptedException {
    final var factory = new CPTestContainerFactory().withTag(tag);

    final var kafka = factory.createKafka();
    //kafka.waitingFor(Wait.forLogMessage("INFO [KafkaServer id=1] started",1));
    kafka.start();
    //Thread.sleep(3_000); //sleep to let logs accumulate
    assertThat(kafka.getLogs().contains("INFO [KafkaServer id=1] started (kafka.server.KafkaServer)")).isTrue();
  }

  @ParameterizedTest(name = "should start Confluent Server v{0}")
  @MethodSource("getCpVersions")
  public void server(String tag) throws InterruptedException {

    final var factory = new CPTestContainerFactory().withTag(tag);

    final var cpServer = factory.createConfluentServer();
    //cpServer.waitingFor(Wait.forLogMessage("INFO [KafkaServer id=1] started",1));
    cpServer.start();
    //Thread.sleep(3_000);
    final var logs = cpServer.getLogs();
    assertThat(logs.contains(String.format("INFO Kafka version: %s-ce", tag))).isTrue();
    assertThat(logs.contains("INFO Kafka startTimeMs:")).isTrue();
    assertThat(logs.contains("INFO [KafkaServer id=1] started (kafka.server.KafkaServer)")).isTrue();

    RestAssured.port = cpServer.getMdsPort();

    // Even without RBAC enabled we should be able to get the Kafka cluster ID as this is part of the REST proxy
    // embedded in Confluent Server.
    given().
        when().
        get("/v1/metadata/id").
        then().
        statusCode(200).
        body("id", is(notNullValue()));
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
    // Even without RBAC enabled we should be able to get the Kafka cluster ID as this is part of the REST proxy
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
