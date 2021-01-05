package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static net.christophschubert.cp.testcontainers.SecurityConfigs.*;

public class KafkaConnectContainer extends CPTestContainer<KafkaConnectContainer> {

    static final int defaultPort = 8083;
    private static final String PROPERTY_PREFIX = "CONNECT";

    private void _configure(KafkaContainer bootstrap) {
        waitingFor(Wait.forHttp("/connectors").forStatusCode(200).forStatusCode(401));
//        waitingFor(Wait.forHttp("/connectors"));
        withNetworkAliases("connect");
        withStartupTimeout(Duration.ofMinutes(5)); //Needs to be placed _after_ call to waitingFor
        withEnv("CONNECT_BOOTSTRAP_SERVERS", getInternalBootstrap(bootstrap));
        withEnv("CONNECT_REST_PORT", "" + httpPort);
        withEnv("CONNECT_GROUP_ID", "connect");
        withEnv("CONNECT_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost");
        withEnv("CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY", "All");
        withEnv("CONNECT_CONFLUENT_TOPIC_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_LISTENERS", getHttpPortListener());
//        withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "WARN");
    //    withEnv("CONNECT_LOG4J_LOGGERS", "org.eclipse.jetty=DEBUG,org.reflections=ERROR,org.apache.kafka.connect=DEBUG");
        withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
        withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs");
        withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets");
        withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status");
        withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java");
        withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    }

    KafkaConnectContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network, defaultPort, PROPERTY_PREFIX);
        _configure(bootstrap);

    }

    protected KafkaConnectContainer(ImageFromDockerfile image, KafkaContainer bootstrap, Network network){
        super(image, bootstrap, network, defaultPort, PROPERTY_PREFIX);
        _configure(bootstrap);
    }

    /**
     * Create a custom Docker image by installing the provided connectors from Confluent Hub.
     *
     * @param connectorNames names of the connectors to install
     * @param baseImageName base image name
     * @return a Docker image with the connectors installed
     */
    public static ImageFromDockerfile customImage(Collection<String> connectorNames, String baseImageName) {
        final var commandPrefix = "confluent-hub install --no-prompt ";
        final String command = connectorNames.stream().map(s -> commandPrefix + s).collect(Collectors.joining(" && "));
        return new ImageFromDockerfile().withDockerfileFromBuilder(builder ->
            builder
                    .from(baseImageName)
                    .run(command)
                    .build()
        );
    }


}
