package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for the Confluent platform components exposing a (REST) HTTP port.
 */
abstract public class CPTestContainer<SELF extends GenericContainer<SELF>> extends GenericContainer<SELF> {
    public static String getInternalBootstrap(KafkaContainer bootstrap) {
        return bootstrap.getNetworkAliases().get(0) + ":9092";
    }

    protected final int httpPort;
    protected final String propertyPrefix;

    CPTestContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network, int httpPort, String propertyPrefix) {
        super(dockerImageName);
        dependsOn(bootstrap);
        withNetwork(network);
        withExposedPorts(httpPort);
        this.httpPort = httpPort;
        this.propertyPrefix = propertyPrefix;
    }


    CPTestContainer(ImageFromDockerfile dockerImage, KafkaContainer bootstrap, Network network, int httpPort, String propertyPrefix) {
        super(dockerImage);
        dependsOn(bootstrap);
        withNetwork(network);
        withExposedPorts(httpPort);
        this.httpPort = httpPort;
        this.propertyPrefix = propertyPrefix;
    }

    CPTestContainer<SELF> withProperty(String property, Object value) {
        final String envVar = propertyPrefix + "_" + property.replace('.', '_').toUpperCase();
        withEnv(envVar, value.toString());
        return this;
    }


    public String getBaseUrl() {
        return String.format("http://%s:%d", getContainerIpAddress(), getMappedPort(httpPort));
    }

    public int getMappedHttpPort() {
        return getMappedPort(httpPort);
    }

    public String getHttpPortListener() {
        return "http://0.0.0.0:" + httpPort;
    }

    public String getInternalBaseUrl() {
        return String.format("http://%s:%d", getNetworkAliases().get(0), httpPort);
    }

    //needed for RBAC enabled components
    private final String containerCertPath = "/tmp/conf";
    private final String localCertPath = "src/main/resources/certs";

    protected void prepareCertificates() {
        withFileSystemBind(localCertPath, containerCertPath);
    }

    protected String getPublicKeyPath() {
        return containerCertPath + "/public.pem";
    }
}
