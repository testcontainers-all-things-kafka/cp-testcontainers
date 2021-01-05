package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import static net.christophschubert.cp.testcontainers.SecurityConfigs.*;

public class ConfluentServerConnectContainer extends KafkaConnectContainer {
    public ConfluentServerConnectContainer(DockerImageName dockerImageName, ConfluentServerContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network);
    }

    ConfluentServerConnectContainer(ImageFromDockerfile image, ConfluentServerContainer bootstrap, Network network) {
        super(image, bootstrap, network);
    }


    public ConfluentServerConnectContainer enableRbac(String mdsServer, String principal, String secret) {
        //TODO: move these to proper location
        final String containerCertPath = "/tmp/conf";
        final String localCertPath = "src/main/resources/certs";

        final var tokenUserCallback = "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler";
        withFileSystemBind(localCertPath, containerCertPath);
        withEnv("CUB_CLASSPATH", "/usr/share/java/confluent-security/connect/*:/usr/share/java/kafka/*:/usr/share/java/cp-base-new/*");

        withProperty("security.protocol", SASL_PLAINTEXT);
        withProperty("sasl.mechanism", OAUTHBEARER);
        withProperty("sasl.login.callback.handler.class", tokenUserCallback);
        withProperty("sasl.jaas.config", oauthJaas(principal, secret, mdsServer));
//        for (var component : List.of("admin", "producer", "consumer")) {
//            withProperty(component + ".security.protocol", SASL_PLAINTEXT);
//            withProperty(component + ".sasl.mechanism", OAUTHBEARER);
//            withProperty(component + ".sasl.login.callback.handler.class", tokenUserCallback);
//            withProperty(component + ".sasl.jaas.config", oauthJaas(principal, secret, mdsServer));
//            System.out.println(oauthJaas(principal, secret, mdsServer));
//        }
        //important: when not configuring secrets, do NOT include `io.confluent.connect.secretregistry.ConnectSecretRegistryExtension`
        // the security extension is definitely needed for RBAC
        withProperty("rest.extension.classes", "io.confluent.connect.security.ConnectSecurityExtension");//,io.confluent.connect.secretregistry.ConnectSecretRegistryExtension");
        //TODO: the following block equals the SR config part, extract to method.
        withProperty("rest.servlet.initializor.classes", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");
        withProperty("public.key.path", containerCertPath + "/public.pem");
        withProperty("confluent.metadata.bootstrap.server.urls", mdsServer);
        withProperty("confluent.metadata.http.auth.credentials.provider", "BASIC");
        withProperty("confluent.metadata.basic.auth.user.info", principal + ":" + secret);
        return this;
    }

    public KafkaConnectContainer enableSecretRegistry() {
        //TODO: implement this!
        return this;
    }
}
