package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.util.Map;

import static net.christophschubert.cp.testcontainers.CPTestContainerFactory.*;

public class RbacEnabledContainerFactory {

    static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    static final String PLAIN = "PLAIN";
    static final String OAUTHBEARER = "OAUTHBEARER";

    final String admin = "admin";
    final String adminSecret = "admin-secret";
    final String containerCertPath = "/tmp/conf";
    final String localCertPath = "src/main/resources/certs";
    final String brokerNetworkAlias = "kafka";

    final Network network;

    public RbacEnabledContainerFactory(Network network) {
        this.network = network;
    }

    public GenericContainer createLdap() {

        return new GenericContainer<>("osixia/openldap:1.3.0")
                .withNetwork(network)
                .withNetworkAliases("ldap")
                .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
                .withEnv("LDAP_ORGANISATION", "Confluent")
                .withEnv("LDAP_DOMAIN", "confluent.io")
                .withFileSystemBind("src/main/resources/ldap", "/container/service/slapd/assets/config/bootstrap/ldif/custom")
                .withCommand("--copy-service"); // add  --loglevel debug to enabel debug info
    }

    public KafkaContainer configureContainerForRBAC(KafkaContainer container) {

        container
                .withNetworkAliases(brokerNetworkAlias)
                .withFileSystemBind(localCertPath, containerCertPath)  //copy certificates
                .withEnv(pToEKafka("super.users"), "User:admin;User:mds;User:alice")
                // KafkaContainer configures two listeners: PLAINTEXT (port 9093), and BROKER (port 9092), BROKER is used for the
                // internal communication on the docker network. We need to configure two SASL mechanisms on BROKER.
                // PLAINTEXT will be used for the communication with external clients, we configure SASL Plain here as well.
                .withEnv(pToEKafka("listener.security.protocol.map"), "PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT")
                .withEnv(pToEKafka("confluent.metadata.security.protocol"), "SASL_PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", PLAIN)
                .withEnv("KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS", "PLAIN,OAUTHBEARER") //Plain for broker<->broker, oauthbearer for cp components<->broker
                // configure inter broker comms and mds<->broker:
                .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG",  formatJaas(admin, adminSecret, Map.of(admin, adminSecret, "mds", "mds-secret")))
                // configure cp-components <-> broker:
                .withEnv(pToEKafka("listener.name.broker.oauthbearer.sasl.server.callback.handler.class"), "io.confluent.kafka.server.plugins.auth.token.TokenBearerValidatorCallbackHandler")
                .withEnv(pToEKafka("listener.name.broker.oauthbearer.sasl.login.callback.handler.class"), "io.confluent.kafka.server.plugins.auth.token.TokenBearerServerLoginCallbackHandler")
                .withEnv(pToEKafka("listener.name.broker.oauthbearer.sasl.jaas.config"), String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required publicKeyPath=\"%s\";", containerCertPath + "/public.pem"))
                // configure communication with external clients
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", PLAIN)
                .withEnv(pToEKafka("listener.name.plaintext.plain.sasl.server.callback.handler.class"), "io.confluent.security.auth.provider.ldap.LdapAuthenticateCallbackHandler")
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", formatJaas(admin, adminSecret))
                // set up authorizer
                .withEnv(pToEKafka("authorizer.class.name"), "io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer")
                // configure MDS
                .withEnv(pToEKafka("confluent.metadata.bootstrap.servers"), String.format("BROKER://%s:9092", brokerNetworkAlias))
                .withEnv(pToEKafka("confluent.metadata.sasl.mechanism"), "PLAIN")
                .withEnv(pToEKafka("confluent.metadata.sasl.jaas.config"), formatJaas("mds", "mds-secret"))
                .withEnv(mdsPrefix("authentication.method"), "BEARER")
                .withEnv(mdsPrefix("listeners"), "http://0.0.0.0:8090")
                .withEnv(mdsPrefix("advertised.listeners"), "http://kafka:8090")
                .withEnv(mdsPrefix("toker.auth.enable"), "true")
                .withEnv(mdsPrefix("token.max.lifetime.ms"), "3600000")
                .withEnv(mdsPrefix("token.signature.algorithm"), "RS256")
                .withEnv(mdsPrefix("token.key.path"), containerCertPath + "/keypair.pem")
                .withEnv(mdsPrefix("public.key.path"), containerCertPath + "/public.pem")

                .withEnv(pToEKafka("confluent.authorizer.access.rule.providers"), "CONFLUENT,ZK_ACL")

                .withEnv("CONFLUENT_METRICS_REPORTER_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
                .withEnv("CONFLUENT_METRICS_REPORTER_SASL_MECHANISM", "PLAIN")
                .withEnv("CONFLUENT_METRICS_REPORTER_SASL_JAAS_CONFIG", formatJaas(admin, adminSecret))

                //configure MDS/LDAP connections
                .withEnv("KAFKA_LDAP_JAVA_NAMING_FACTORY_INITIAL", "com.sun.jndi.ldap.LdapCtxFactory")
                .withEnv("KAFKA_LDAP_COM_SUN_JNDI_LDAP_READ_TIMEOUT", "3000")
                .withEnv("KAFKA_LDAP_JAVA_NAMING_PROVIDER_URL", "ldap://ldap:389")
                .withEnv("KAFKA_LDAP_JAVA_NAMING_SECURITY_PRINCIPAL", "cn=admin,dc=confluent,dc=io")
                .withEnv("KAFKA_LDAP_JAVA_NAMING_SECURITY_CREDENTIALS", "admin")
                .withEnv("KAFKA_LDAP_JAVA_NAMING_SECURITY_AUTHENTICATION", "simple")
                .withEnv("KAFKA_LDAP_USER_SEARCH_BASE", "ou=users,dc=confluent,dc=io")
                .withEnv("KAFKA_LDAP_GROUP_SEARCH_BASE", "ou=groups,dc=confluent,dc=io")
                .withEnv("KAFKA_LDAP_USER_NAME_ATTRIBUTE", "uid")
                .withEnv("KAFKA_LDAP_USER_OBJECT_CLASS", "inetOrgPerson")
                .withEnv("KAFKA_LDAP_USER_MEMBEROF_ATTRIBUTE", "ou")
                .withEnv("KAFKA_LDAP_GROUP_MEMBER_ATTRIBUTE", "memberUid")
                .withEnv("KAFKA_LDAP_GROUP_NAME_ATTRIBUTE", "cn")
                .withEnv("KAFKA_LDAP_GROUP_OBJECT_CLASS", "posixGroup")
                .withEnv("KAFKA_LDAP_GROUP_MEMBER_ATTRIBUTE_PATTERN", "cn=(.*),ou=users,dc=confluent,dc=io");

        return container;
    }

    public SchemaRegistryContainer configureContainerForRBAC(SchemaRegistryContainer schemaRegistryContainer) {

        final String srPrincipal = "sr-user";
        final String srSecret = "sr-user-secret";
        final var mdsBootstrap = String.format("http://%s:8090", brokerNetworkAlias);
        final var saslJaasConfig =
                String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                        "username=\"%s\" password=\"%s\" metadataServerUrls=\"%s\";",
                        srPrincipal, srSecret, mdsBootstrap);

        schemaRegistryContainer
                .withFileSystemBind(localCertPath, containerCertPath)
                .withEnv("CUB_CLASSPATH", "/usr/share/java/confluent-security/schema-registry/*:/usr/share/java/schema-registry/*:/usr/share/java/cp-base-new/*")
                .withProperty("kafkastore.security.protocol", SASL_PLAINTEXT)
                .withProperty("kafkastore.sasl.mechanism", OAUTHBEARER)
                .withProperty("kafkastore.sasl.login.callback.handler.class", "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler")
                .withProperty("kafkastore.sasl.jaas.config", saslJaasConfig)
                .withProperty("debug", true)
                .withProperty("schema.registry.resource.extension.class", "io.confluent.kafka.schemaregistry.security.SchemaRegistrySecurityResourceExtension")
                .withProperty("confluent.schema.registry.authorizer.class", "io.confluent.kafka.schemaregistry.security.authorizer.rbac.RbacAuthorizer")
                .withProperty("rest.servlet.initializor.classes", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler")
                .withProperty("confluent.metadata.bootstrap.server.urls", mdsBootstrap)
                .withProperty("confluent.metadata.http.auth.credentials.provider", "BASIC")
                .withProperty("confluent.metadata.basic.auth.user.info", srPrincipal + ":" + srSecret)
                .withProperty("public.key.path", containerCertPath + "/public.pem");
        return schemaRegistryContainer;
    }

    String mdsPrefix(String property) {
        return pToEKafka("confluent.metadata.server." + property);
    }

}
