package net.christophschubert.cp.testcontainers;

import org.junit.Assert;
import org.junit.Test;

public class LdapContainerTest {
    @Test
    public void startLdap() {
        final var ldap = new LdapContainer();
        ldap.start();
        final var logs = ldap.getLogs();

        Assert.assertTrue(logs.contains("adding new entry \"cn=alice,ou=users,dc=confluent,dc=io\"\n"));
        Assert.assertTrue(logs.contains("adding new entry \"cn=barney,ou=users,dc=confluent,dc=io\"\n"));
    }
}
