package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

/**
 *
 */
abstract public class CPTestContainer<SELF extends GenericContainer<SELF>> extends GenericContainer<SELF> {
    public static String getInternalBootstrap(KafkaContainer bootstrap) {
        return bootstrap.getNetworkAliases().get(0) + ":9092";
    }

    protected final int httpPort;

    CPTestContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network, int httpPort) {
        super(dockerImageName);
        dependsOn(bootstrap);
        withNetwork(network);
        withExposedPorts(httpPort);
        this.httpPort = httpPort;
    }

    CPTestContainer(ImageFromDockerfile dockerImage, KafkaContainer bootstrap, Network network, int httpPort) {
        super(dockerImage);
        dependsOn(bootstrap);
        withNetwork(network);
        withExposedPorts(httpPort);
        this.httpPort = httpPort;
    }

    public String getBaseUrl() {
        return String.format("http://%s:%d", getContainerIpAddress(), getMappedPort(httpPort));
    }

    public String httpPortListener() {
        return "http://0.0.0.0:" + httpPort;
    }

    public String getInternalBaseUrl() {
        return String.format("http://%s:%d", getNetworkAliases().get(0), httpPort);
    }
}
