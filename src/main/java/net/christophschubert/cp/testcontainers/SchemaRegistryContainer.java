package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static net.christophschubert.cp.testcontainers.SecurityConfigs.OAUTHBEARER;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.SASL_PLAINTEXT;

public class SchemaRegistryContainer extends CPTestContainer<SchemaRegistryContainer> {

    static final int defaultPort = 8081;

    SchemaRegistryContainer(DockerImageName imageName, KafkaContainer bootstrap, Network network) {
        super(imageName, bootstrap, network, defaultPort, "SCHEMA_REGISTRY");
        waitingFor(Wait.forHttp("/subjects").forStatusCodeMatching(it -> it >= 200 && it < 300 || it == 401));
        withStartupTimeout(Duration.ofMinutes(2)); //Needs to be placed _after_ call to waitingFor
        withProperty("host.name", "schema-registry");
        withProperty("kafkastore.bootstrap.servers", getInternalBootstrap(bootstrap));
        withProperty("listeners", getHttpPortListener());
    }


    public SchemaRegistryContainer enableRbac() {
        return enableRbac("http://kafka:8090", "sr-user", "sr-user-secret");

    }

    //TODO: move these to proper location

    final String containerCertPath = "/tmp/conf";
    final String localCertPath = "src/main/resources/certs";


    public SchemaRegistryContainer enableRbac(String mdsBootstrap, String srPrincipal, String srSecret) {

        final var saslJaasConfig =
                String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                                "username=\"%s\" password=\"%s\" metadataServerUrls=\"%s\";",
                        srPrincipal, srSecret, mdsBootstrap);


        withFileSystemBind(localCertPath, containerCertPath);
        withEnv("CUB_CLASSPATH", "/usr/share/java/confluent-security/schema-registry/*:/usr/share/java/schema-registry/*:/usr/share/java/cp-base-new/*");
        withProperty("kafkastore.security.protocol", SASL_PLAINTEXT);
        withProperty("kafkastore.sasl.mechanism", OAUTHBEARER);
        withProperty("kafkastore.sasl.login.callback.handler.class", "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler");
        withProperty("kafkastore.sasl.jaas.config", saslJaasConfig);
        withProperty("debug", true);
        withProperty("schema.registry.resource.extension.class", "io.confluent.kafka.schemaregistry.security.SchemaRegistrySecurityResourceExtension");
        withProperty("confluent.schema.registry.authorizer.class", "io.confluent.kafka.schemaregistry.security.authorizer.rbac.RbacAuthorizer");
        withProperty("rest.servlet.initializor.classes", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");
        withProperty("confluent.metadata.bootstrap.server.urls", mdsBootstrap);
        withProperty("confluent.metadata.http.auth.credentials.provider", "BASIC");
        withProperty("confluent.metadata.basic.auth.user.info", srPrincipal + ":" + srSecret);
        withProperty("public.key.path", containerCertPath + "/public.pem");
        return this;
    }

}
