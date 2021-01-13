package net.christophschubert.cp.testcontainers;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.Assert.*;

public class SecurityConfigsTest {
    @Test
    public void formatPlainJaas() {
        final var jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" ;";
        assertEquals(jaas, SecurityConfigs.plainJaas("admin", "admin-secret"));
    }

    @Test
    public void formatPlainJaasAdditional() {
        final var jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" password=\"admin-secret\" user_producer=\"producer-secret\" user_consumer=\"consumer-secret\";";
        final LinkedHashMap<String, String> info = new LinkedHashMap<>();
        info.put("producer", "producer-secret");
        info.put("consumer", "consumer-secret");
        assertEquals(jaas,
                SecurityConfigs.plainJaas("admin", "admin-secret", info));

    }

}