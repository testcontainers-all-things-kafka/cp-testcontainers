package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LdapContainer extends GenericContainer<LdapContainer> {

    static final DockerImageName baseImageName = DockerImageName.parse("osixia/openldap");
    static final String defaultTag = "1.3.0";

    static final String ldifBootstrapPath = "/container/service/slapd/assets/config/bootstrap/ldif/custom/custom.ldif";

    private static final Set<String> DEFAULT_USERS = Set.of("alice", "barney");
    private static final Set<String> DEFAULT_GROUPS = Set.of("administrators", "developers");
    private static final Map<String, Set<String>> DEFAULT_MEMBERS =
            Map.of("alice", Set.of("administrators"), "barney", Set.of("developers"));

    public LdapContainer() {
        this(DEFAULT_USERS, DEFAULT_GROUPS, DEFAULT_MEMBERS);
    }

    public LdapContainer(Set<String> users) {
        this(users.stream().collect(Collectors.toMap(s -> s, s -> s + "-secret")), DEFAULT_GROUPS, DEFAULT_MEMBERS);
    }

    /**
     * @param users   - Collection of user names
     * @param groups  - Collection of group names
     * @param members - Maps user to multiple groups
     */
    public LdapContainer(Set<String> users, Set<String> groups, Map<String, Set<String>> members) {
        this(users.stream().collect(Collectors.toMap(s -> s, s -> s + "-secret")), groups, members);
    }

    public LdapContainer(Map<String, String> ldapUsers, Set<String> groups, Map<String, Set<String>> members) {
        super(buildImage(formatLdif(ldapUsers, groups, members)));
        _configure();
    }

    private static ImageFromDockerfile buildImage(String ldif) {
        return new ImageFromDockerfile().withFileFromString("custom.ldif", ldif)
                .withDockerfileFromBuilder(LdapContainer::dockerfileBuild);
    }

    private static String dockerfileBuild(DockerfileBuilder db) {
        return db.from(baseImageName.getUnversionedPart() + ":" + defaultTag)
                .copy("custom.ldif", ldifBootstrapPath).build();
    }

    private void _configure() {
        withNetworkAliases("ldap");
        withEnv("LDAP_ORGANISATION", "Confluent");
        withEnv("LDAP_DOMAIN", "confluent.io");
        withCommand("--copy-service --loglevel debug");
        waitingFor(Wait.forLogMessage(".*slapd starting.*", 2));
    }

    private static final String HEADER =
            "dn: ou=users,dc=confluent,dc=io\n" +
                    "objectClass: organizationalUnit\n" +
                    "ou: Users\n" +
                    "\n" +
                    "dn: ou=groups,dc=confluent,dc=io\n" +
                    "objectClass: organizationalUnit\n" +
                    "ou: Groups\n\n";

    static String formatLdif(Map<String, String> ldapUsers, Set<String> ldapGroups, Map<String, Set<String>> members) {

        final StringBuilder builder = new StringBuilder(HEADER);

        var groupIdNumber = 5000;

        for (String entry : ldapGroups) {
            builder.append(formatGroupEntry(entry, groupIdNumber++));
        }

        var userIdNumber = 10000;

        for (Map.Entry<String, String> entry : ldapUsers.entrySet()) {
            builder.append(formatUserEntry(entry.getKey(), entry.getValue(),
                    userIdNumber++, members.getOrDefault(entry.getKey(), Set.of())));
        }

        return builder.toString();
    }

    private static final String GROUP_TEMPLATE =
            "dn: cn=%s,ou=groups,{{ LDAP_BASE_DN }}\n" +
                    "objectClass: top\n" +
                    "objectClass: posixGroup\n" +
                    "cn: %s\n" +
                    "gidNumber: %d\n\n";

    private static String formatGroupEntry(String groupName, int gidNumber) {
        return String.format(GROUP_TEMPLATE, groupName, groupName, gidNumber);
    }

    private static final String USER_TEMPLATE =
            "dn: cn=%s,ou=users,{{ LDAP_BASE_DN }}\n" +
                    "objectClass: inetOrgPerson\n" +
                    "objectClass: posixAccount\n" +
                    "%s" +
                    "uid: %s\n" +
                    "cn: %s\n" +
                    "sn: Snow\n" +
                    "uidNumber: %d\n" +
                    "gidNumber: 5000\n" +
                    "userPassword: %s\n" +
                    "homeDirectory: /home/%s\n\n";

    private static String formatUserEntry(String username, String password, int uidNumber, Set<String> members) {
        return String.format(USER_TEMPLATE, username, assembleMembers(members),
                username, username, uidNumber, password, username);
    }

    private static String assembleMembers(Set<String> members) {
        return members.stream()
                .map(s -> String.format("memberOf: cn=%s,ou=groups,{{ LDAP_BASE_DN }}\n", s))
                .collect(Collectors.joining(""));
    }
}