package net.christophschubert.cp.testcontainers;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class SecurityConfigsTest {
    @Test
    public void formatPlainJaas() {
        final var jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" ;";
        assertThat(SecurityConfigs.plainJaas("admin", "admin-secret")).isEqualTo(jaas);
    }

    @Test
    public void formatPlainJaasAdditional() {
        final var jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" password=\"admin-secret\" user_producer=\"producer-secret\" user_consumer=\"consumer-secret\";";
        final LinkedHashMap<String, String> info = new LinkedHashMap<>();
        info.put("producer", "producer-secret");
        info.put("consumer", "consumer-secret");
        assertThat(jaas).isEqualTo(
            SecurityConfigs.plainJaas("admin", "admin-secret", info));

    }

}