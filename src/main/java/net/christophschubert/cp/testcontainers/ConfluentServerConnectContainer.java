package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static net.christophschubert.cp.testcontainers.ContainerConfigs.CUB_CLASSPATH;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.*;

public class ConfluentServerConnectContainer extends KafkaConnectContainer {
    public ConfluentServerConnectContainer(DockerImageName dockerImageName, ConfluentServerContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network);
    }

    ConfluentServerConnectContainer(ImageFromDockerfile image, ConfluentServerContainer bootstrap, Network network) {
        super(image, bootstrap, network);
    }


    public ConfluentServerConnectContainer enableRbac(String mdsServer, String principal, String secret) {

        prepareCertificates();
        withEnv(CUB_CLASSPATH, "/usr/share/java/confluent-security/connect/*:/usr/share/java/kafka/*:/usr/share/java/cp-base-new/*");

        // configure access to broker via OAuth
        withProperties(oAuthWithTokenCallbackHandlerProperties(principal, secret, mdsServer));

        //option 2
        // need to configure these properties, even they are the some as for the connect worker.
        // Otherwise, e.g,  the `producer.override.sasl.jaas.config` of a source
        // connector will not be picked up and hence it is not possible to start a connect with a user principal.
        for (var prefix: List.of("producer", "consumer", "admin")) {
            withProperty(prefix + ".security.protocol", "SASL_PLAINTEXT");
            withProperty(prefix + ".sasl.mechanism", "OAUTHBEARER");
            withProperty(prefix + ".sasl.login.callback.handler.class", "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler");
        }
        //important: when not configuring secrets, do NOT include `io.confluent.connect.secretregistry.ConnectSecretRegistryExtension`
        // the security extension is definitely needed for RBAC
        withProperty("rest.extension.classes", "io.confluent.connect.security.ConnectSecurityExtension");//,io.confluent.connect.secretregistry.ConnectSecretRegistryExtension");
        //TODO: the following block equals the SR config part, extract to method.
        withProperty("rest.servlet.initializor.classes", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");
        withProperty("public.key.path", getPublicKeyPath());
        withProperties(confluentMdsSettings(principal, secret, mdsServer));
        return this;
    }

    public KafkaConnectContainer enableSecretRegistry() {
        //TODO: implement this!
        return this;
    }
}
