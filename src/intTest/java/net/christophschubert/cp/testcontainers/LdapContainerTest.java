package net.christophschubert.cp.testcontainers;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LdapContainerTest {
    @Test
    public void startLdap() {
        final var ldap = new LdapContainer();
        ldap.start();
        final var logs = ldap.getLogs();

        assertThat(logs.contains("adding new entry \"cn=alice,ou=users,dc=confluent,dc=io\"\n")).isTrue();
        assertThat(logs.contains("adding new entry \"cn=barney,ou=users,dc=confluent,dc=io\"\n")).isTrue();
    }
}
