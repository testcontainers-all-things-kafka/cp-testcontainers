package net.christophschubert.cp.testcontainers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CPTestContainerFactoryTest {

    @Test
    public void ptoETest() {
        assertEquals("COMPONENT_TEST_PROPERTY", CPTestContainerFactory.pToE("COMPONENT", "test.property"));
    }

    @Test
    public void pToEKafkaTest() {
        assertEquals("KAFKA_TEST_PROPERTY", CPTestContainerFactory.pToEKafka("test.property"));
    }


}