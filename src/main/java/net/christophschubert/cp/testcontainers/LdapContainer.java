package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LdapContainer extends GenericContainer<LdapContainer> {
    static final DockerImageName baseImageName = DockerImageName.parse("osixia/openldap");
    static final String defaultTag = "1.3.0";

    static final String ldifBootstrapPath = "/container/service/slapd/assets/config/bootstrap/ldif/custom/custom.ldif";

    public LdapContainer() {
        this(Set.of("alice",  "barney", "sr-user" ));
    }

    public LdapContainer(Set<String> usernames) {
        this(usernames.stream().collect(Collectors.toMap(s -> s, s -> s + "-secret")));
    }

    public LdapContainer(Map<String, String> ldapUsers) {
        super(new ImageFromDockerfile().withFileFromString("custom.ldif", formatLdif(ldapUsers)).withDockerfileFromBuilder(
                db -> db.from(baseImageName.getUnversionedPart() + ":" + defaultTag).copy("custom.ldif", ldifBootstrapPath).build()));
        _configure();
    }

    public LdapContainer(String pathToLdif) {
        super(baseImageName.withTag(defaultTag));
        withCopyFileToContainer(MountableFile.forHostPath(pathToLdif), ldifBootstrapPath);
        _configure();
    }

    private void _configure() {
          withNetworkAliases("ldap");
          withEnv("LDAP_ORGANISATION", "Confluent");
          withEnv("LDAP_DOMAIN", "confluent.io");
          withCommand("--copy-service --loglevel debug");
          waitingFor(Wait.forLogMessage(".*slapd starting.*", 2));
    }

    static String formatLdif(Map<String, String> ldapUsers) {
        final String header = "dn: ou=users,dc=confluent,dc=io\n" +
                "objectClass: organizationalUnit\n" +
                "ou: Users\n" +
                "\n" +
                "dn: ou=groups,dc=confluent,dc=io\n" +
                "objectClass: organizationalUnit\n" +
                "ou: Groups\n\n";
        StringBuilder builder = new StringBuilder(header);

        var groups = List.of("Kafka Developers", "ProjectA", "ProjectB");
        final var startGidNumber = 5000;
        for (int i = 0; i < groups.size(); ++i) {
            builder.append(formatGroupEntry(groups.get(i), startGidNumber + i));
        }

        var uidNumber = 10000;
        for (Map.Entry<String, String> entry : ldapUsers.entrySet()) {
            builder.append(formatUserEntry(entry.getKey(), entry.getValue(), uidNumber++));
        }

        return builder.toString();
    }

    static String formatGroupEntry(String groupName, int gidNumber) {
        final var template = "dn: cn=%s,ou=groups,{{ LDAP_BASE_DN }}\n" +
                "objectClass: top\n" +
                "objectClass: posixGroup\n" +
                "cn: %s\n" +
                "gidNumber: %d\n\n";
        return String.format(template, groupName, groupName, gidNumber);
    }

    static String formatUserEntry(String username, String password, int uidNumber) {
        final var template = "dn: cn=%s,ou=users,{{ LDAP_BASE_DN }}\n" +
                "objectClass: inetOrgPerson\n" +
                "objectClass: posixAccount\n" +
                "uid: %s\n" +
                "cn: %s\n" +
                "sn: Snow\n" +
                "uidNumber: %d\n" +
                "gidNumber: 5000\n" +
                "userPassword: %s\n" +
                "homeDirectory: /home/%s\n\n";
        return String.format(template, username, username, username, uidNumber, password, username);
    }

}
