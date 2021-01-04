package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.stream.Collectors;

public class CPTestContainerFactory {

    String repository = "confluentinc";
    String tag = "6.0.1";

    Network network;

    public CPTestContainerFactory(Network network) {
        Objects.requireNonNull(network);
        this.network = network;
    }

    public CPTestContainerFactory() {
        this(Network.newNetwork());
    }


    public KafkaContainer createKafka() {
        return new KafkaContainer(imageName("cp-kafka")).withNetwork(network);
    }


    public KafkaContainer createCPServer() {
        final int mdsPort= 8090;
        final var imageName = imageName("cp-server").asCompatibleSubstituteFor("confluentinc/cp-kafka");
        return new KafkaContainer(imageName)
                .withNetwork(network)
                .withExposedPorts(mdsPort, KafkaContainer.KAFKA_PORT)
                .withEnv(pToEKafka("confluent.metadata.topic.replication.factor"), "1")
                .withEnv(pToEKafka("confluent.license.topic.replication.factor"), "1")
                .withEnv(pToEKafka("confluent.metadata.bootstrap.servers"), "BROKER://kafka:9092")
                .withEnv("CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS", "1")
                .withEnv("CONFLUENT_TELEMETRY_ENABLED", "false")
                .withEnv("KAFKA_CONFLUENT_TELEMETRY_ENABLED", "false")
                .withEnv("KAFKA_METRIC_REPORTERS"," io.confluent.metrics.reporter.ConfluentMetricsReporter")
                .withEnv("CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS", "kafka:9092");

    }

    /**
     * Create a new Kafka container with SASL PLAIN authentication enabled.
     *
     * Their will be one user admin with password admin-secret configured implicitly.
     *
     * @param userAndPasswords additional users and their passwords.
     * @return a KafkaContainer with external SASL Plain listener
     */
    public KafkaContainer createKafkaSaslPlain(Map<String, String> userAndPasswords) {
        return createKafkaSaslPlain(userAndPasswords, false);
    }

    public KafkaContainer createKafkaSaslPlain(Map<String, String> userAndPasswords, boolean enableAuthorizationViaAcls) {
        // The testcontainer Kafka module specifies two listeners PLAINTEXT and BROKER.
        // The advertised listener of PLAINTEXT is mapped to a port on localhost.
        // For Confluent Platform components running in the same Docker network as the broker we need to use the BROKER listener
        // (which should really be called INTERNAL).
        //
        // see https://www.testcontainers.org/modules/kafka/ for details

        final String admin = "admin";
        final String adminSecret = "admin-secret";
        final Map<String, String> userInfo = new HashMap<>(userAndPasswords);
        userInfo.put(admin, adminSecret);

        final var kafka =  new KafkaContainer(imageName("cp-kafka"))
                .withNetwork(network)
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG",  formatJaas(admin, adminSecret, Map.of(admin, adminSecret)))
                .withEnv("KAFKA_SASL_JAAS_CONFIG", formatJaas(admin, adminSecret, Collections.emptyMap()))
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", formatJaas(admin, adminSecret, userInfo));
        if (enableAuthorizationViaAcls) {
            //Remark: should use kafka.security.auth.SimpleAclAuthorizer for tags BEFORE 5.4.0.
            //Since this is pretty ancient by now, no logic for choosing the right authorizer is implemented.
            kafka.withEnv(pToEKafka("authorizer.class.name"), "kafka.security.authorizer.AclAuthorizer")
                .withEnv(pToEKafka("super.users"), "User:" + admin);
        }

        return kafka;
    }

    public SchemaRegistryContainer createSchemaRegistry(KafkaContainer bootstrap) {
        return new SchemaRegistryContainer(imageName("cp-schema-registry"), bootstrap, network);
    }

    public KafkaConnectContainer createKafkaConnect(KafkaContainer bootstrap) {
        return new KafkaConnectContainer(imageName("cp-kafka-connect"), bootstrap, network);
    }

    public KafkaConnectContainer createReplicator(KafkaContainer bootstrap) {
        return new KafkaConnectContainer(imageName("cp-enterprise-replicator"), bootstrap, network);
    }

    public KafkaConnectContainer createCustomConnector(String hubConnector, KafkaContainer bootstrap) {
        return createCustomConnector(Collections.singleton(hubConnector), bootstrap);
    }

    public KafkaConnectContainer createCustomConnector(Set<String> hubComponents, KafkaContainer bootstrap) {
        final var baseImageName = repository + "/cp-kafka-connect-base:" + tag;
        final var image = KafkaConnectContainer.customImage(hubComponents, baseImageName);
        return new KafkaConnectContainer(image, bootstrap, network).withEnv("CONNECT_PLUGIN_PATH", "/usr/share/confluent-hub-components");
    }

    public RestProxyContainer createRestProxy(KafkaContainer bootstrap) {
        return new RestProxyContainer(imageName("cp-kafka-rest"), bootstrap, network);
    }

    /**
     * Creates a ksqlDB server instance using the version bundled with the specified version of Confluent Platform.
     *
     * @param bootstrap Kafka container to use as bootstrap server
     * @return a ksqlDB container
     */
    public KsqlDBContainer createKsqlDB(KafkaContainer bootstrap) {
        return new KsqlDBContainer(imageName("cp-ksqldb-server"), bootstrap, network);
    }

    /**
     * Creates a ksqlDB server container using a independently released ksqlDB image version.
     *
     * @param bootstrap Kafka container to use as bootstrap server
     * @param tag the version number of the ksqlDB server image to use (e.g. 0.14.0 or latest)
     * @return a ksqlDB container
     */
    public KsqlDBContainer createKsqDB(KafkaContainer bootstrap, String tag) {
        final var imageName = DockerImageName.parse(String.format("%s/ksqldb-server:%s", repository, tag));
        return new KsqlDBContainer(imageName, bootstrap, network);
    }

    DockerImageName imageName(String componentName) {
        return DockerImageName.parse(String.format("%s/%s:%s", repository, componentName, tag));
    }

    // helper methods
    public static String formatJaas(String user, String password, Map<String, String> additionalUsers) {
        final var collectUsers = additionalUsers.entrySet().stream()
                .map(e -> String.format("user_%s=\"%s\"", e.getKey(), e.getValue()))
                .collect(Collectors.joining(" "));
        return String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\" %s;",
                user, password, collectUsers);
    }

    public static String formatJaas(String user, String password) {
        return formatJaas(user, password, Collections.emptyMap());
    }

    /**
     * Translates property keys to environment variables.
     *
     * @param componentPrefix prefix of the component, e.g. KAFKA, CONNECT, etc
     * @param propertyName name of the original property
     * @return environment variable corresponding to the property as expected by Docker container configure scripts
     */
    static String pToE(String componentPrefix, String propertyName) {
        return componentPrefix + "_" + propertyName.replace('.', '_').toUpperCase();
    }

    static String pToEKafka(String propertyName) {
        return pToE("KAFKA", propertyName);
    }


}

