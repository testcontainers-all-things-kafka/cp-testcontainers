package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.stream.Collectors;

import static net.christophschubert.cp.testcontainers.CPTestContainerFactory.pToEKafka;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.PLAIN;
import static net.christophschubert.cp.testcontainers.SecurityConfigs.plainJaas;

public class SalsPlainDecorator {

    Map<String, String> userAndPasswords = Collections.emptyMap();
    List<String> additionalSuperUsers = Collections.emptyList();

    /**
     * Create a decorator which will add the given users.
     *
     * One user 'admin' with password 'admin-secret' will be added automatically.
     *
     * @param userAndPasswords users to be added
     * @param additionalSuperUsers list of users to make super-users when authorization is enabled
     */
    public SalsPlainDecorator(Map<String, String> userAndPasswords, List<String> additionalSuperUsers) {
        this.userAndPasswords = userAndPasswords;
        this.additionalSuperUsers = additionalSuperUsers;
    }

    public SalsPlainDecorator(Map<String, String> userAndPasswords) {
        this.userAndPasswords = userAndPasswords;
    }

    public SalsPlainDecorator() {
    }

    String getAuthorizer(KafkaContainer container) {
        //TODO: implement test
        //Remark: should use kafka.security.auth.SimpleAclAuthorizer for tags BEFORE 5.4.0.
        if (container instanceof ConfluentServerContainer)
            return "io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer";
        final var tag = DockerImageName.parse(container.getDockerImageName()).getVersionPart();
        //This comparison should be valid until we reach a major version number of 10
        return tag.compareTo("5.4.0") < 0 ? "kafka.security.auth.SimpleAclAuthorizer" : "kafka.security.authorizer.AclAuthorizer";
    }

    public <T extends KafkaContainer> T addSaslPlainConfig(T container) {
        return addSaslPlainConfig(container, false);
    }

    public <T extends KafkaContainer> T addSaslPlainConfig(T container, boolean enableAuthorizationViaAcls) {

        // The testcontainer Kafka module specifies two listeners PLAINTEXT and BROKER.
        // The advertised listener of PLAINTEXT is mapped to a port on localhost.
        // For Confluent Platform components running in the same Docker network as the broker we need to use the BROKER listener
        // (which should really be called INTERNAL).
        //
        // See https://www.testcontainers.org/modules/kafka/ for details

        final String admin = "admin";
        final String adminSecret = "admin-secret";
        final Map<String, String> userInfo = new HashMap<>(userAndPasswords);
        userInfo.put(admin, adminSecret);

        final List<String> superUsers = new ArrayList<>(additionalSuperUsers);
        superUsers.add(admin);

        container
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", PLAIN)
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", PLAIN)
                .withEnv("KAFKA_LISTENER_NAME_BROKER_SASL_ENABLED_MECHANISMS", PLAIN)
                .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG",  plainJaas(admin, adminSecret, Map.of(admin, adminSecret)))
                .withEnv("KAFKA_SASL_JAAS_CONFIG", plainJaas(admin, adminSecret, Collections.emptyMap()))
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", plainJaas(admin, adminSecret, userInfo));
        if (enableAuthorizationViaAcls) {
            final String su = superUsers.stream().map(s -> "User:" + s).collect(Collectors.joining(";"));
            container.withEnv(pToEKafka("authorizer.class.name"), getAuthorizer(container))
                    .withEnv(pToEKafka("super.users"), su);
        }

        return container;
    }
}
