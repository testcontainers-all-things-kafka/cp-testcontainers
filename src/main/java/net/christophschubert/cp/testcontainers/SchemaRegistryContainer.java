package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends CPTestContainer<SchemaRegistryContainer> {

    final int defaultPort = 8081;

    SchemaRegistryContainer(DockerImageName imageName, KafkaContainer bootstrap, Network network) {
        super(imageName, bootstrap, network);

        withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", getInternalBootstrap(bootstrap));
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + defaultPort);
        withExposedPorts(defaultPort);
    }

    public String getBaseUrl() {
        return String.format("http://%s:%d", getContainerIpAddress(), getMappedPort(defaultPort));
    }

    public String getInternalBaseUrl() {
        return String.format("http://%s:%d", getNetworkAliases().get(0), defaultPort);
    }
}
