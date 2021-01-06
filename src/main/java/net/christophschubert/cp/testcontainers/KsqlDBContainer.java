package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import static net.christophschubert.cp.testcontainers.ContainerConfigs.CUB_CLASSPATH;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.*;

/**
 * Test-container instance of ksqlDB.
 */
public class KsqlDBContainer extends CPTestContainer<KsqlDBContainer> {

    static final int defaultPort = 8088;

    KsqlDBContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network, defaultPort, "KSQL");

        withProperty("host.name", "ksqldb-server");
        withProperty("bootstrap.servers", getInternalBootstrap(bootstrap));
        withProperty("listeners", getHttpPortListener());
        withEnv("KSQL_CACHE_MAX_BYTES_BUFFERING", "0");
        waitingFor(Wait.forHttp("/").forStatusCode(200).forStatusCode(401));
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
     * <p>
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


    public KsqlDBContainer enableRbac(String mdsBootstrap, String ksqlPrincipal, String ksqlSecret) {
        //TODO: should check if this causes any errors with some of the base images
        prepareCertificates();
        withEnv(CUB_CLASSPATH, "/usr/share/java/confluent-security/ksql/*:/usr/share/java/ksqldb-server/*:/usr/share/java/cp-base-new/*");
        withProperty("ksql.security.extension.class", "io.confluent.ksql.security.KsqlConfluentSecurityExtension");

        //configure access to broker
        withProperties(oAuthWithTokenCallbackHandlerProperties(ksqlPrincipal, ksqlSecret, mdsBootstrap));

//       # Enable KSQL OAuth authentication
        // TODO: why do we have these two settings?
        withProperty("rest.servlet.initializor.classes", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");
        withProperty("websocket.servlet.initializor.classes", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");
        withProperty("oauth.jwt.public.key.path", getPublicKeyPath());
        // TODO: does this property still exists? Double-check
        withProperty("confluent.metadata.public.key.path", getPublicKeyPath());
        withProperty("ksql.authentication.plugin.class", "io.confluent.ksql.security.VertxBearerOrBasicAuthenticationPlugin");
        // TODO: I should double-check all the setting here with the official docs
        withProperty("confluent.schema.registry.authorizer.class", "io.confluent.kafka.schemaregistry.security.authorizer.rbac.RbacAuthorizer");
        withProperty("rest.servlet.initializor.classes", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");



        withProperties(confluentMdsSettings(ksqlPrincipal, ksqlSecret, mdsBootstrap));
        withProperty("public.key.path", getPublicKeyPath());
        // Venki's page (https://confluentinc.atlassian.net/wiki/spaces/~730978534/pages/1402262129/RBAC+Implementation)
        // has this setting:
//        withProperty("confluent.metadata.public.key.path", getPublicKeyPath());

        // TODO: this property was configures in some of the sources: a google search yields no results: double-check
        withProperty("confluent.metadata.basic.auth.credentials.provider", USER_INFO);
        //access to connect seems to be missing
        withEnv("KSQL_LOG4J_ROOT_LOGLEVEL", "INFO");

        withProperty("ksql.schema.registry.basic.auth.credentials.source", USER_INFO); //not mentioned in https://docs.confluent.io/platform/current/security/rbac/ksql-rbac.html
        withProperty("ksql.schema.registry.basic.auth.user.info",ksqlPrincipal + ":" + ksqlSecret);

        // this seems to be a stabler way to find out whether a ksqlDB cluster is ready
        // maybe use this as the default health check
        waitingFor(Wait.forLogMessage(".*INFO Server up and running.*", 1));

        return this;
    }
}
