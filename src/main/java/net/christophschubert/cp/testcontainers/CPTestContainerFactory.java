package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.*;

import static net.christophschubert.cp.testcontainers.SecurityConfigs.PLAIN;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.plainJaas;

public class CPTestContainerFactory {

    String repository = "confluentinc";

    String tag = "6.0.1";

    final Network network;

    public CPTestContainerFactory(Network network) {
        Objects.requireNonNull(network);
        this.network = network;
    }

    public CPTestContainerFactory() {
        this(Network.newNetwork());
    }

    public String getTag() {
        return tag;
    }

    /**
     * Set the tag (i.e. version) used to create docker images
     * @param tag the tag, e.g. 6.0.1 or 5.5.1
     */
    public CPTestContainerFactory withTag(String tag) {
        this.tag = tag;
        return this;
    }

    public LdapContainer createLdap(Set<String> userNames) {
        return new LdapContainer(userNames).withNetwork(network);
    }

    public LdapContainer createLdap() {
        return new LdapContainer().withNetwork(network);
    }

    public KafkaContainer createKafka() {
        return new KafkaContainer(imageName("cp-kafka")).withNetwork(network);
    }


    public ConfluentServerContainer createConfluentServer() {
        return (ConfluentServerContainer) new ConfluentServerContainer(tag).withNetwork(network);
    }

    public SchemaRegistryContainer createSchemaRegistry(KafkaContainer bootstrap) {
        return new SchemaRegistryContainer(imageName("cp-schema-registry"), bootstrap, network);
    }

    public KafkaConnectContainer createKafkaConnect(KafkaContainer bootstrap) {
        return new KafkaConnectContainer(imageName("cp-kafka-connect"), bootstrap, network);
    }

    public ConfluentServerConnectContainer createConfluentServerConnect(ConfluentServerContainer bootstrap) {
        return new ConfluentServerConnectContainer(imageName("cp-server-connect"), bootstrap, network);
    }

    public ConfluentServerConnectContainer createConfluentServerConnect(Collection<String> confluentHubComponents, ConfluentServerContainer bootstrap) {
        final var baseImageName = repository + "/cp-server-connect-base:" + tag;
        final var image = KafkaConnectContainer.customImage(confluentHubComponents, baseImageName);
        return (ConfluentServerConnectContainer) new ConfluentServerConnectContainer(image, bootstrap, network)
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/confluent-hub-components");
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

