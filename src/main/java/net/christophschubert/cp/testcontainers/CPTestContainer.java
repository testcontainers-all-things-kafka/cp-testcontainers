package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for the Confluent platform components exposing a (REST) HTTP port.
 */
abstract public class CPTestContainer<SELF extends GenericContainer<SELF>> extends GenericContainer<SELF> {
    public static String getInternalBootstrap(KafkaContainer bootstrap) {
        return bootstrap.getNetworkAliases().get(0) + ":9092";
    }

    protected final int httpPort;
    protected final String propertyPrefix;

    final KafkaContainer bootstrap;

    CPTestContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network, int httpPort, String propertyPrefix) {
        super(dockerImageName);
        this.bootstrap = bootstrap;
        dependsOn(bootstrap);
        withNetwork(network);
        withExposedPorts(httpPort);
        this.httpPort = httpPort;
        this.propertyPrefix = propertyPrefix;
    }


    CPTestContainer(ImageFromDockerfile dockerImage, KafkaContainer bootstrap, Network network, int httpPort, String propertyPrefix) {
        super(dockerImage);
        this.bootstrap = bootstrap;
        dependsOn(bootstrap);
        withNetwork(network);
        withExposedPorts(httpPort);
        this.httpPort = httpPort;
        this.propertyPrefix = propertyPrefix;
    }

    CPTestContainer<SELF> withProperty(String property, Object value) {
        Objects.requireNonNull(value);
        final String envVar = propertyPrefix + "_" + property.replace('.', '_').toUpperCase();
        withEnv(envVar, value.toString());
        return this;
    }

    CPTestContainer<SELF> withProperties(Map<String, Object> properties) {
        properties.forEach(this::withProperty);
        return this;
    }

    CPTestContainer<SELF> withProperties(String prefix, Map<String, Object> properties) {
        properties.forEach((k, v) -> withProperty(prefix + "." + k, v));
        return this;
    }

    public abstract CPTestContainer<SELF> withLogLevel(String logLevel);


    public String getBaseUrl() {
        return String.format("http://%s:%d", getHost(), getMappedPort(httpPort));
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

    //tools for debugging

    /**
     * Prints the environment variables of the container in a format suitable for inclusion in a docker-compose.yaml.
     */
    public void prettyPrintEnvs() {
        getEnvMap().entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).forEach(e -> System.out.printf("%s: '%s'%n", e.getKey(), e.getValue()));
    }
}
