package net.christophschubert.cp.testcontainers;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CPTestContainerFactoryTest {

    @Test
    public void ptoETest() {
        assertThat(CPTestContainerFactory.pToE("COMPONENT", "test.property")).isEqualTo("COMPONENT_TEST_PROPERTY");
    }

    @Test
    public void pToEKafkaTest() {
        assertThat(CPTestContainerFactory.pToEKafka("test.property")).isEqualTo("KAFKA_TEST_PROPERTY");
    }


}