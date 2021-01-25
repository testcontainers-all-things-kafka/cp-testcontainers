package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaConnectContainer extends CPTestContainer<KafkaConnectContainer> {

    static final int defaultPort = 8083;
    private static final String PROPERTY_PREFIX = "CONNECT";

    private final String configsPostfix = "-configs";
    private final String offsetsPostfix = "-offsets";
    private final String statusPostfix = "-status";


    private String clusterId = "connect";

    public Set<String> getInternalTopics() {
        return Set.of(
                clusterId + configsPostfix, clusterId + offsetsPostfix, clusterId + statusPostfix
        );
    }

    public KafkaConnectContainer withReplicationFactors(int rf) {
        withProperty("replication.factor", rf);
        withProperty("confluent.topic.replication.factor", rf);
        withProperty("config.storage.replication.factor", rf);
        withProperty("offset.storage.replication.factor", rf);
        withProperty("status.storage.replication.factor", rf);
        return this;
    }

    public KafkaConnectContainer withClusterId(String clusterId) {
        withProperty("group.id", clusterId);
        withProperty("config.storage.topic", clusterId + configsPostfix);
        withProperty("offset.storage.topic", clusterId + offsetsPostfix);
        withProperty("status.storage.topic", clusterId + statusPostfix);
        return this;
    }

    public String getClusterId() {
        return clusterId;
    }

    private void _configure(KafkaContainer bootstrap) {
        waitingFor(Wait.forHttp("/connectors").forStatusCode(200).forStatusCode(401)); // 401 will be return if RBAC is configured
        withStartupTimeout(Duration.ofMinutes(5)); //Needs to be placed _after_ call to waitingFor
        withProperty("bootstrap.servers", getInternalBootstrap(bootstrap));
        withProperty("rest.port", "" + httpPort);
        withClusterId(clusterId);
        withReplicationFactors(1);
        withProperty("rest.advertised.host.name", "localhost"); //TODO: change to getHost()
        withProperty("connector.client.config.override.policy", "All");
        withProperty("listeners", getHttpPortListener());
//        withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "WARN");
    //    withEnv("CONNECT_LOG4J_LOGGERS", "org.eclipse.jetty=DEBUG,org.reflections=ERROR,org.apache.kafka.connect=DEBUG");
        withProperty("plugin.path", "/usr/share/java");
        withProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        withProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
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
