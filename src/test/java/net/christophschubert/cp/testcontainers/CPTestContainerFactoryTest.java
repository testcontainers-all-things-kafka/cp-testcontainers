package net.christophschubert.cp.testcontainers;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CPTestContainerFactoryTest {

    @Test
    public void formatJaas() {
        final var jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" ;";
        assertEquals(jaas, CPTestContainerFactory.formatJaas("admin", "admin-secret"));
    }

    @Test
    public void formatJaasAdditional() {
        final var jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"admin\" password=\"admin-secret\" user_producer=\"producer-secret\" user_consumer=\"consumer-secret\";";
        final LinkedHashMap<String, String> info = new LinkedHashMap<>();
        info.put("producer", "producer-secret");
        info.put("consumer", "consumer-secret");
        assertEquals(jaas,
                CPTestContainerFactory.formatJaas("admin", "admin-secret", info));

    }


}