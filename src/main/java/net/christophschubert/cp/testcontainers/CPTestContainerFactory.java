package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CPTestContainerFactory {

    String repository = "confluentinc";

    String tag = "7.8.0";

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
        try (LdapContainer ldapContainer = new LdapContainer(userNames)) {
            return ldapContainer.withNetwork(network);
        }
    }

    public LdapContainer createLdap() {
        try (LdapContainer ldapContainer = new LdapContainer()) {
            return ldapContainer.withNetwork(network);
        }
    }   

    private void configureContainerResources(org.testcontainers.containers.GenericContainer<?> container) {
        container
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                .withMemory(1024L * 1024L * 1024L) // 1GB memory limit
                .withMemorySwap(1536L * 1024L * 1024L)) // 1.5GB swap
            .withStartupTimeout(java.time.Duration.ofMinutes(5));
    }

    public KafkaContainer createKafka() {
        KafkaContainer container = new KafkaContainer(imageName("cp-kafka"));
        container.withNetwork(network);
        configureContainerResources(container);
        return container;
    }

    public ConfluentServerContainer createConfluentServer() {
        ConfluentServerContainer container = new ConfluentServerContainer(repository, tag);
        container.withNetwork(network);
        configureContainerResources(container);
        return container;
    }

    public SchemaRegistryContainer createSchemaRegistry(KafkaContainer bootstrap) {
        SchemaRegistryContainer container = new SchemaRegistryContainer(imageName("cp-schema-registry"), bootstrap, network);
        configureContainerResources(container);
        return container;
    }

    public KafkaConnectContainer createKafkaConnect(KafkaContainer bootstrap) {
        KafkaConnectContainer container = new KafkaConnectContainer(imageName("cp-kafka-connect"), bootstrap, network);
        configureContainerResources(container);
        return container;
    }

    public ConfluentServerConnectContainer createConfluentServerConnect(ConfluentServerContainer bootstrap) {
        ConfluentServerConnectContainer container = new ConfluentServerConnectContainer(imageName("cp-server-connect"), bootstrap, network);
        configureContainerResources(container);
        return container;
    }

    public ConfluentServerConnectContainer createConfluentServerConnect(Collection<String> confluentHubComponents, ConfluentServerContainer bootstrap) {
        final var baseImageName = repository + "/cp-server-connect-base:" + tag;
        final var image = KafkaConnectContainer.customImage(confluentHubComponents, baseImageName);
        ConfluentServerConnectContainer container = new ConfluentServerConnectContainer(image, bootstrap, network);
        container.withEnv("CONNECT_PLUGIN_PATH", "/usr/share/confluent-hub-components");
        configureContainerResources(container);
        return container;
    }

    public KafkaConnectContainer createReplicator(KafkaContainer bootstrap) {
        KafkaConnectContainer container = new KafkaConnectContainer(imageName("cp-enterprise-replicator"), bootstrap, network);
        configureContainerResources(container);
        return container;
    }

    public KafkaConnectContainer createCustomConnector(String hubComponent, KafkaContainer bootstrap) {
        return createCustomConnector(Collections.singleton(hubComponent), bootstrap);
    }

    public KafkaConnectContainer createCustomConnector(Set<String> hubComponents, KafkaContainer bootstrap) {
        final var baseImageName = repository + "/cp-kafka-connect-base:" + tag;
        final var image = KafkaConnectContainer.customImage(hubComponents, baseImageName);
        KafkaConnectContainer container = new KafkaConnectContainer(image, bootstrap, network);
        container.withEnv("CONNECT_PLUGIN_PATH", "/usr/share/confluent-hub-components");
        configureContainerResources(container);
        return container;
    }

    public RestProxyContainer createRestProxy(KafkaContainer bootstrap) {
        RestProxyContainer container = new RestProxyContainer(imageName("cp-kafka-rest"), bootstrap, network);
        configureContainerResources(container);
        return container;
    }

    /**
     * Creates a ksqlDB server instance using the version bundled with the specified version of Confluent Platform.
     *
     * @param bootstrap Kafka container to use as bootstrap server
     * @return a ksqlDB container
     */
    public KsqlDBContainer createKsqlDB(KafkaContainer bootstrap) {
        KsqlDBContainer container = new KsqlDBContainer(imageName("cp-ksqldb-server"), bootstrap, network);
        configureContainerResources(container);
        return container;
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
        KsqlDBContainer container = new KsqlDBContainer(imageName, bootstrap, network);
        configureContainerResources(container);
        return container;
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


    public static class ClusterSpec<C extends KafkaContainer> {
        public final List<ZooKeeperContainer> zks;
        public final List<C> kafkas;

        public ClusterSpec(List<ZooKeeperContainer> zks, List<C> kafkas) {
            this.zks = zks;
            this.kafkas = kafkas;
        }

        public void startAll() {
            try {
                Startables.deepStart(kafkas).get();
            } catch (InterruptedException | ExecutionException e) {
                final var msg = String.format("Error starting up %s", kafkas);
                throw new RuntimeException(msg, e.getCause());
            }
        }

        public String getInternalBootstrap() {
            return null;
        }

        public String getBootstrap() {
            return kafkas.stream().map(c -> "localhost:" + c.getMappedPort(KafkaContainer.KAFKA_PORT)).collect(Collectors.joining(","));
        }
    }

    /**
     * Generate a random string of given length.
     * @param length of the String to be generated
     * @return a random lowercase string
     */
    String generateRandom (int length) {
        return new Random()
                .ints(length, 'a', 'z' + 1)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    public ClusterSpec<KafkaContainer> createKafkaCluster(int numBrokers) {
        if (numBrokers < 1) {
            throw new IllegalArgumentException(String.format("numBrokers should be non-negative (was %d)", numBrokers));
        }
        final ZooKeeperContainer zk = new ZooKeeperContainer(repository, tag).withNetwork(network);

        zk.start();// starting zk is necessary to configure zookeeper connect
        final int rf = Math.min(3, numBrokers);
        final var clusterPrefix = generateRandom(7);

        final var kafkas = IntStream.rangeClosed(1, numBrokers).mapToObj(id ->
                createKafka().
                        withNetwork(network).
                        dependsOn(zk).
                        withExternalZookeeper(zk.getInternalConnect()).
                        withEnv("KAFKA_BROKER_ID", "" + id).
                        withNetworkAliases(clusterPrefix + "-kafka-" + id)
        ).map(
                kafkaContainer -> KafkaContainerTools.adjustReplicationFactors(kafkaContainer, rf)
        ).collect(Collectors.toList());
        return new ClusterSpec<>(List.of(zk), kafkas);
    }

    public ClusterSpec<ConfluentServerContainer> createConfluentServerCluster(int numServers) {
        if (numServers < 1) {
            throw new IllegalArgumentException(String.format("numBrokers should be non-negative (was %d)", numServers));
        }
        final ZooKeeperContainer zk = new ZooKeeperContainer(repository, tag).withNetwork(network);

        zk.start();// starting zk is necessary to configure zookeeper connect
        final int rf = Math.min(3, numServers);
        final var clusterPrefix = generateRandom(7);

        final var servers = IntStream.rangeClosed(1, numServers).mapToObj(id ->
                (ConfluentServerContainer)createConfluentServer().
                        withNetwork(network).
                        dependsOn(zk).
                        withExternalZookeeper(zk.getInternalConnect()).
                        withEnv("KAFKA_BROKER_ID", "" + id).
                        withNetworkAliases(clusterPrefix + "cp-server-" + id)
        ).map(
                kafkaContainer -> KafkaContainerTools.adjustReplicationFactors(kafkaContainer, rf)
        ).collect(Collectors.toList());
        return new ClusterSpec<>(List.of(zk), servers);
    }
}
