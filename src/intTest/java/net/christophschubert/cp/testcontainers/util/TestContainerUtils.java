package net.christophschubert.cp.testcontainers.util;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

import java.util.Arrays;
import java.util.Map;
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

    /**
     * Prints the environment variables of the container in a format suitable for inclusion in a docker-compose.yaml.
     */
    public static void prettyPrintEnvs(GenericContainer<?> c) {
        c.getEnvMap().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> System.out.printf("%s: '%s'%n", e.getKey(), e.getValue()));
    }
}
