package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Test-container instance of ksqlDB.
 */
public class KsqlDBContainer extends CPTestContainer<KsqlDBContainer> {

    static final int defaultPort = 8088;

    KsqlDBContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network, defaultPort, "KSQL");

        withEnv("SCHEMA_REGISTRY_HOST_NAME", "ksqldb-server");
        withEnv("KSQL_BOOTSTRAP_SERVERS", getInternalBootstrap(bootstrap));
        withEnv("KSQL_LISTENERS", getHttpPortListener());
        withEnv("KSQL_CACHE_MAX_BYTES_BUFFERING", "0");
        waitingFor(Wait.forHttp("/"));
    }

    /**
     * Sets the service Id of the ksqlDB server. The service Id is used as prefix to the internal topics.
     *
     * @param serviceId the service Id.
     * @return this container
     */
    public KsqlDBContainer withServiceId(String serviceId) {
        withEnv("KSQL_KSQL_SERVICE_ID", serviceId);
        return this;
    }

    /**
     * Prepare ksqlDB container to start in headless mode with the provided query-file.
     *
     * No HTTP port will be opened when starting in headless mode.
     *
     * @param queriesFile the path to the file
     * @return this container
     */
    public KsqlDBContainer withQueriesFile(String queriesFile) {
        final String containerPath = "/queries.sql";
        withCopyFileToContainer(MountableFile.forHostPath(queriesFile), containerPath);
        withEnv("KSQL_KSQL_QUERIES_FILE", containerPath);
        waitingFor(Wait.forLogMessage(".*INFO Server up and running.*", 1)); // no HTTP port wil be opened in headless mode
        return this;
    }

    /**
     * Configures ksqlDB to use the provided schema registry.
     *
     * @param schemaRegistry the schema registry to use
     * @return this container
     */
    public KsqlDBContainer withSchemaRegistry(SchemaRegistryContainer schemaRegistry) {
        withEnv("KSQL_KSQL_SCHEMA_REGISTRY_URL", schemaRegistry.getInternalBaseUrl());
        dependsOn(schemaRegistry);
        return this;
    }

    /**
     * Configures ksqlDB to use the provided Kafka Connect container.
     *
     * @param connectContainer the Kafka Connect container to use.
     * @return this container
     */
    public KsqlDBContainer withConnect(KafkaConnectContainer connectContainer) {
        withEnv("KSQL_KSQL_CONNECT_URL", connectContainer.getInternalBaseUrl());
        dependsOn(connectContainer);
        return this;
    }
}
