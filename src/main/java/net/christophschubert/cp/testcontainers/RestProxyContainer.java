package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class RestProxyContainer extends CPTestContainer<RestProxyContainer> {

    final int defaultPort = 8082;

    RestProxyContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network);

        withEnv("KAFKA_REST_HOST_NAME", "restproxy");
        withEnv("KAFKA_REST_BOOTSTRAP_SERVERS", getInternalBootstrap(bootstrap));
        withEnv("KAFKA_REST_LISTENERS", "http://0.0.0.0:" + defaultPort);
        withExposedPorts(defaultPort);
    }

    public String getBaseUrl() {
        return String.format("http://%s:%d", getContainerIpAddress(), getMappedPort(defaultPort));
    }
}
