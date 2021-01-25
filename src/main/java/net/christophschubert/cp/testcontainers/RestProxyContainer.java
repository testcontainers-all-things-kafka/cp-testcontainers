package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class RestProxyContainer extends CPTestContainer<RestProxyContainer> {

    static final int defaultPort = 8082;

    RestProxyContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network, defaultPort, "KAFKA_REST");

        withEnv("KAFKA_REST_HOST_NAME", "restproxy");
        withEnv("KAFKA_REST_BOOTSTRAP_SERVERS", getInternalBootstrap(bootstrap));
        withEnv("KAFKA_REST_LISTENERS", getHttpPortListener());
    }

    public RestProxyContainer withSchemaRegistry(SchemaRegistryContainer schemaRegistry) {
        withProperty("schema.registry.url", schemaRegistry.getInternalBaseUrl());
        dependsOn(schemaRegistry);
        return this;
    }

    @Override
    public CPTestContainer<RestProxyContainer> withLogLevel(String logLevel) {
        //TODO: find out how to configure logging
        return this;
    }
}
