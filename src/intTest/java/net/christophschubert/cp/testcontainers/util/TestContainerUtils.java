package net.christophschubert.cp.testcontainers.util;

import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class TestContainerUtils {
    public static void startAll(Startable... containers) {
        try {
            Startables.deepStart(Arrays.stream(containers)).get();
        } catch (InterruptedException | ExecutionException e) {
            final var msg = String.format("Error starting up %s", Arrays.asList(containers));
            throw new RuntimeException(msg, e.getCause());
        }
    }
}
