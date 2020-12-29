package net.christophschubert.cp.testcontainers;

import org.junit.Test;
import org.testcontainers.containers.Network;

public class KsqlDBContainerTest {
    @Test
    public void setupKsqlDB() {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var ksqlDB = containerFactory.createKsqlDB(kafka);
        ksqlDB.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        ksqlDB.start();
    }
}
