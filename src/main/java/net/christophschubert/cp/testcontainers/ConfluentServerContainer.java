package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static net.christophschubert.cp.testcontainers.CPTestContainerFactory.pToEKafka;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.*;

public class ConfluentServerContainer extends KafkaContainer {
    final int mdsPort = 8090;

    final String admin = "admin";
    final String adminSecret = "admin-secret";

    public ConfluentServerContainer(String repository, String tag) {
        super(DockerImageName.parse(repository + "/cp-server:" + tag).asCompatibleSubstituteFor(repository + "/cp-kafka"));
        withExposedPorts(mdsPort, KafkaContainer.KAFKA_PORT); //mdsPort doubles as port for embedded (admin, V3) REST proxy
        withStartupTimeout(Duration.ofMinutes(4));

        KafkaContainerTools.adjustReplicationFactors(this, 1);
        setReplicationFactors(1);


        withProperty("confluent.telemetry.enabled", false);
        withProperty("confluent.metrics.enabled", false);
        withProperty("confluent.balancer.enable", false);
    }

    ConfluentServerContainer setReplicationFactors(int rf) {
        withProperty("confluent.metadata.topic.replication.factor", rf);
        withProperty("confluent.license.topic.replication.factor", rf);
        withEnv("KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR", "" + rf); // also sets the _confluent-telemetry-metrics topic RF
        withEnv("CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS", "" + rf);
        return this;
    }

    ConfluentServerContainer withProperty(String property, Object value) {
        Objects.requireNonNull(value);
        final String envVar = "KAFKA_" + property.replace('.', '_').toUpperCase();
        withEnv(envVar, value.toString());
        return this;
    }


    public int getMdsPort() {
        return getMappedPort(mdsPort);
    }

    /**
     * Assumes that there's an LDAP server with network alias ldap running on the same Docker network.
     *
     * @return this container, with RBAC configured
     */
    public ConfluentServerContainer enableRbac() {

        final String containerCertPath = "/tmp/conf";
        final String localCertPath = "src/main/resources/certs";

        // this should be set for all Kafka container at startup-time already
        final String brokerNetworkAlias = "kafka"; //getNetworkAliases().get(0);
        withNetworkAliases(brokerNetworkAlias);
        withFileSystemBind(localCertPath, containerCertPath);  //copy certificates

        withProperty("super.users", "User:admin;User:mds;User:alice");
        // KafkaContainer configures two listeners: PLAINTEXT (port 9093), and BROKER (port 9092), BROKER is used for the
        // internal communication on the docker network. We need to configure two SASL mechanisms on BROKER.
        // PLAINTEXT will be used for the communication with external clients, we configure SASL Plain here as well.
        withProperty("listener.security.protocol.map", "PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT");
        withProperty("confluent.metadata.security.protocol", SASL_PLAINTEXT);
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");
        withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", PLAIN);
        withEnv("KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS", "PLAIN,OAUTHBEARER"); //Plain for broker<->broker, oauthbearer for cp components<->broker
        // configure inter broker comms and mds<->broker:
        withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG", plainJaas(admin, adminSecret, Map.of(admin, adminSecret, "mds", "mds-secret")));
        // configure cp-components <-> broker:
        withEnv(pToEKafka("listener.name.broker.oauthbearer.sasl.server.callback.handler.class"), "io.confluent.kafka.server.plugins.auth.token.TokenBearerValidatorCallbackHandler");
        withEnv(pToEKafka("listener.name.broker.oauthbearer.sasl.login.callback.handler.class"), "io.confluent.kafka.server.plugins.auth.token.TokenBearerServerLoginCallbackHandler");
        withEnv(pToEKafka("listener.name.broker.oauthbearer.sasl.jaas.config"), String.format("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required publicKeyPath=\"%s\";", containerCertPath + "/public.pem"));
        // configure communication with external clients
        withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", PLAIN);
        withEnv(pToEKafka("listener.name.plaintext.plain.sasl.server.callback.handler.class"), "io.confluent.security.auth.provider.ldap.LdapAuthenticateCallbackHandler");
        withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", plainJaas(admin, adminSecret));
        // these docs: https://docs.confluent.io/platform/current/kafka/authentication_sasl/client-authentication-ldap.html only provide "org.apache.kafka.common.security.plain.PlainLoginModule required;"
        // set up authorizer
        withEnv(pToEKafka("authorizer.class.name"), "io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer");
        // configure MDS
        withEnv(pToEKafka("confluent.metadata.bootstrap.servers"), String.format("BROKER://%s:9092", brokerNetworkAlias));
        withEnv(pToEKafka("confluent.metadata.sasl.mechanism"), PLAIN);
        withEnv(pToEKafka("confluent.metadata.sasl.jaas.config"), plainJaas("mds", "mds-secret"));
        withEnv(mdsPrefix("authentication.method"), "BEARER");
        withEnv(mdsPrefix("listeners"), "http://0.0.0.0:8090");
        withEnv(mdsPrefix("advertised.listeners"), String.format("http://%s:8090", brokerNetworkAlias));
        withEnv(mdsPrefix("token.auth.enable"), "true");
        withEnv(mdsPrefix("token.max.lifetime.ms"), "7200000"); //TODO: had to set it to 2 hours to prevent re-login problems: look into this!
        withEnv(mdsPrefix("token.signature.algorithm"), "RS256");
        withEnv(mdsPrefix("token.key.path"), containerCertPath + "/keypair.pem");
        withEnv(mdsPrefix("public.key.path"), containerCertPath + "/public.pem");

        withEnv(pToEKafka("confluent.authorizer.access.rule.providers"), "CONFLUENT,ZK_ACL");

        // env var names starting with CONFLUENT_METRICS_ will be taken over as well
        withEnv("CONFLUENT_METRICS_REPORTER_SECURITY_PROTOCOL", SASL_PLAINTEXT);
        withEnv("CONFLUENT_METRICS_REPORTER_SASL_MECHANISM", PLAIN);
        withEnv("CONFLUENT_METRICS_REPORTER_SASL_JAAS_CONFIG", plainJaas(admin, adminSecret));

        //configure MDS/LDAP connections
        withEnv("KAFKA_LDAP_JAVA_NAMING_FACTORY_INITIAL", "com.sun.jndi.ldap.LdapCtxFactory");
        withEnv("KAFKA_LDAP_COM_SUN_JNDI_LDAP_READ_TIMEOUT", "3000");
        withEnv("KAFKA_LDAP_JAVA_NAMING_PROVIDER_URL", "ldap://ldap:389");
        withEnv("KAFKA_LDAP_JAVA_NAMING_SECURITY_PRINCIPAL", "cn=admin,dc=confluent,dc=io");
        withEnv("KAFKA_LDAP_JAVA_NAMING_SECURITY_CREDENTIALS", "admin");
        withEnv("KAFKA_LDAP_JAVA_NAMING_SECURITY_AUTHENTICATION", "simple");
        withEnv("KAFKA_LDAP_USER_SEARCH_BASE", "ou=users,dc=confluent,dc=io");
        withEnv("KAFKA_LDAP_GROUP_SEARCH_BASE", "ou=groups,dc=confluent,dc=io");
        withEnv("KAFKA_LDAP_USER_NAME_ATTRIBUTE", "uid");
        withEnv("KAFKA_LDAP_USER_OBJECT_CLASS", "inetOrgPerson");
        withEnv("KAFKA_LDAP_USER_MEMBEROF_ATTRIBUTE", "ou");
        withEnv("KAFKA_LDAP_GROUP_MEMBER_ATTRIBUTE", "memberUid");
        withEnv("KAFKA_LDAP_GROUP_NAME_ATTRIBUTE", "cn");
        withEnv("KAFKA_LDAP_GROUP_OBJECT_CLASS", "posixGroup");
        withEnv("KAFKA_LDAP_GROUP_MEMBER_ATTRIBUTE_PATTERN", "cn=(.*),ou=users,dc=confluent,dc=io");

        return this;
    }

    String mdsPrefix(String property) {
        return pToEKafka("confluent.metadata.server." + property);
    }

    public String licenseTopic() {
        return "_confluent-license";
    }
}
