package net.christophschubert.cp.testcontainers.util;

import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ConsumerLoop {
    public static <K, V> List<V> loopUntil(Consumer<K, V> consumer, int maxRecords) {
        return loopUntil(consumer, maxRecords, Duration.ofSeconds(1), 60);
    }

    public static <K, V> List<V> loopUntil(Consumer<K, V> consumer, int maxRecords, Duration timeout, int tries) {
        final List<V> results = new ArrayList<>();
        for (int i = 0; i < tries; ++i) {
            final var records = consumer.poll(timeout);
            records.forEach(r -> results.add(r.value()));
            if (results.size() >= maxRecords)
                return results;
        }
        return results;
    }
}
