package net.christophschubert.cp.testcontainers;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LdapContainerTest {

    @Test
    public void testLdapConfig() {
        final var config = LdapContainer.formatLdif(Map.of("greta", "secret"),
                Set.of("admins"), Map.of("greta", Set.of("admins", "undefined")));

        final var lines = List.of(config.split("\n"));

        assertTrue(lines.contains("dn: cn=greta,ou=users,{{ LDAP_BASE_DN }}"), "user is not defined");

        assertTrue(lines.contains("memberOf: cn=admins,ou=groups,{{ LDAP_BASE_DN }}"), "memberOf missing");
        assertTrue(lines.contains("dn: cn=admins,ou=groups,{{ LDAP_BASE_DN }}"), "group is not defined");

        assertTrue(lines.contains("memberOf: cn=undefined,ou=groups,{{ LDAP_BASE_DN }}"), "memberOf missing");
        assertFalse(lines.contains("dn: cn=undefined,ou=groups,{{ LDAP_BASE_DN }}"), "user is defined");
    }

    @Test
    public void testLdapConfigWhenNoMembers() {
        final var config = LdapContainer.formatLdif(Map.of("greta", "secret"),
                Set.of("admins"), Map.of());

        final var lines = List.of(config.split("\n"));

        assertTrue(lines.contains("dn: cn=greta,ou=users,{{ LDAP_BASE_DN }}"), "user is not defined");
        assertTrue(lines.contains("dn: cn=admins,ou=groups,{{ LDAP_BASE_DN }}"), "group is not defined");

        assertFalse(config.contains("memberOf"), "memberOf defined");
    }
}
