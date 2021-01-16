package net.christophschubert.cp.testcontainers;

import org.junit.Test;

import java.util.List;

public class MultiVersionTest {
    @Test
    public void skeleton() {
        for (var tag : List.of("5.4.1", "5.5.1", "6.0.1")) {
            final var factory = new CPTestContainerFactory().withTag(tag);

            final var cServer = factory.createConfluentServer().enableRbac();
            cServer.start();
            // start the container and write some interesting assertion, see if they fail
        }
    }
}
