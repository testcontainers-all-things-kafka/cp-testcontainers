package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static net.christophschubert.cp.testcontainers.ContainerConfigs.CUB_CLASSPATH;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.*;

public class SchemaRegistryContainer extends CPTestContainer<SchemaRegistryContainer> {

    static final int defaultPort = 8081;
    private String clusterId = "schema-registry";

    SchemaRegistryContainer(DockerImageName imageName, KafkaContainer bootstrap, Network network) {
        super(imageName, bootstrap, network, defaultPort, "SCHEMA_REGISTRY");
        waitingFor(Wait.forHttp("/subjects").forStatusCodeMatching(it -> it >= 200 && it < 300 || it == 401));
        withStartupTimeout(Duration.ofMinutes(2)); //Needs to be placed _after_ call to waitingFor
        withProperty("host.name", "schema-registry");
        withProperty("kafkastore.bootstrap.servers", getInternalBootstrap(bootstrap));
        withProperty("listeners", getHttpPortListener());
    }

    public SchemaRegistryContainer withClusterId(String clusterId) {
        this.clusterId = clusterId;
        withProperty("schema.registry.group.id", clusterId);
        return this;
    }

    public String getClusterId() {
        return clusterId;
    }


    public SchemaRegistryContainer enableRbac() {
        if (! (bootstrap instanceof ConfluentServerContainer))
            throw new IllegalStateException("rbac requires a ConfluenServerContainer as bootstrap");
        return enableRbac(((ConfluentServerContainer) bootstrap).getMdsUrl(), "sr-user", "sr-user-secret");
    }


    public SchemaRegistryContainer enableRbac(String mdsBootstrap, String srPrincipal, String srSecret) {

        prepareCertificates();
        withEnv(CUB_CLASSPATH, "/usr/share/java/confluent-security/schema-registry/*:/usr/share/java/schema-registry/*:/usr/share/java/cp-base-new/*");

        //configure access to broker via OAuth
        withProperties("kafkastore", oAuthWithTokenCallbackHandlerProperties(srPrincipal, srSecret, mdsBootstrap));

        withProperty("debug", true);
        withProperty("schema.registry.resource.extension.class", "io.confluent.kafka.schemaregistry.security.SchemaRegistrySecurityResourceExtension");
        withProperty("confluent.schema.registry.authorizer.class", "io.confluent.kafka.schemaregistry.security.authorizer.rbac.RbacAuthorizer");
        withProperty("rest.servlet.initializor.classes", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");
        //TODO: question: the docs at https://docs.confluent.io/platform/current/schema-registry/security/rbac-schema-registry.html#schemaregistry-rbac
        // mention to use `confluent.schema.registry.auth.mechanism=JETTY_AUTH` as the recommended auth mechanism when using RBAC

        withProperties(confluentMdsSettings(srPrincipal, srSecret, mdsBootstrap));

        withProperty("public.key.path", getPublicKeyPath()); //

        // TODO: compare with
        // https://github.com/confluentinc/cp-demo/blob/6.0.1-post/docker-compose.yml
        return this;
    }

}
