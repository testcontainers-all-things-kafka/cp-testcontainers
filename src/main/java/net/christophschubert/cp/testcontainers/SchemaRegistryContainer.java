package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends CPTestContainer<SchemaRegistryContainer> {

    static final int defaultPort = 8081;

    SchemaRegistryContainer(DockerImageName imageName, KafkaContainer bootstrap, Network network) {
        super(imageName, bootstrap, network, defaultPort);

        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", getInternalBootstrap(bootstrap));
        withEnv("SCHEMA_REGISTRY_LISTENERS", httpPortListener());
    }

}
