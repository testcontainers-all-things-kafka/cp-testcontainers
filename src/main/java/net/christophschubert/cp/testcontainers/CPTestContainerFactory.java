package net.christophschubert.cp.testcontainers;

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

    DockerImageName imageName(String componentName) {
        return DockerImageName.parse(String.format("%s/%s:%s", repository, componentName, tag));
    }

    public KafkaContainer createKafka() {
        return new KafkaContainer(imageName("cp-kafka")).withNetwork(network);
    }

    /**
     * Create a new Kafka container with SASL PLAIN authentication enabled.
     *
     * Their will be one user admin with password admin-secret configured implicitly.
     *
     * @param userAndPasswords additional users and their passwords.
     * @return
     */
    public KafkaContainer createKafkaSaslPlain(Map<String, String> userAndPasswords) {
        // The testcontainer Kafka module specifies two listeners PLAINTEXT and BROKER.
        // The advertised listener of PLAINTEXT is mapped two a port on localhost.
        // For Confluent Platform components running in the same Docker network as the broker we need to use the BROKER listener
        // (which should really be called INTERNAL).
        //
        // see https://www.testcontainers.org/modules/kafka/ for details

        final String admin = "admin";
        final String adminSecret = "admin-secret";
        final Map<String, String> userInfo = new HashMap<>(userAndPasswords);
        userInfo.put(admin, adminSecret);

        return new KafkaContainer(imageName("cp-kafka"))
                .withNetwork(network)
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG",  formatJaas(admin, adminSecret, Map.of(admin, adminSecret)))
                .withEnv("KAFKA_SASL_JAAS_CONFIG", formatJaas(admin, adminSecret, Collections.emptyMap()))
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", formatJaas(admin, adminSecret, userInfo));
        //TODO: enable authorization

    }

    static String formatJaas(String user, String password, Map<String, String> additionalUsers) {
        final var collectUsers = additionalUsers.entrySet().stream()
                .map(e -> String.format("user_%s=\"%s\"", e.getKey(), e.getValue()))
                .collect(Collectors.joining(" "));
        return String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\" %s;",
                user, password, collectUsers);
    }

    static String formatJaas(String user, String password) {
        return formatJaas(user, password, Collections.emptyMap());
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
     * @param tag the version number of the ksqlDB server image to use(e.g. 0.14.0 or latest)
     * @return a ksqlDB container
     */
    public KsqlDBContainer createKsqDB(KafkaContainer bootstrap, String tag) {
        final var imageName = DockerImageName.parse(String.format("%s/ksqldb-server:%s", repository, tag));
        return new KsqlDBContainer(imageName, bootstrap, network);
    }

}
