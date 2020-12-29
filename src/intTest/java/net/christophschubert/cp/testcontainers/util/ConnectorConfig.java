package net.christophschubert.cp.testcontainers.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Minimal implementation, see https://github.com/christophschubert/kafka-connect-java-client for a
 * more complete implementation.
 */
public class ConnectorConfig {

    public static final String CONNECTOR_CLASS_CONFIG = "connector.class";
    public static final String TASKS_MAX_CONFIG = "tasks.max";
    public static final String TOPICS_CONFIG = "topics"; // sink connectors only
    public static final String TOPIC_REGEX_CONFIG= "topic.regex"; // sink connectors only
    public static final String KEY_CONVERTER_CONFIG = "key.converter";
    public static final String VALUE_CONVERTER_CONFIG = "value.converter";
    public static final String HEADER_CONVERTER_CONFIG = "header.converter";

    //lightweight builder pattern
    public static ConnectorConfig sink(String name, String connectorClassName) {
        final Map<String, Object> config = new HashMap<>();
        config.put(CONNECTOR_CLASS_CONFIG, connectorClassName);
        return new ConnectorConfig(name, config);
    }

    //lightweight builder pattern
    public static ConnectorConfig source(String name, String connectorClassName) {
        return sink(name, connectorClassName); //stub implementation
    }


    public ConnectorConfig withTopics(Collection<String> topicNames) {
        config.put(TOPICS_CONFIG, String.join(",", topicNames));
        return this;
    }

    public ConnectorConfig withTopicRegex(String topicRegex) {
        config.put(TOPIC_REGEX_CONFIG, topicRegex);
        return this;
    }

    public ConnectorConfig withKeyConverter(String keyConverter) {
        config.put(KEY_CONVERTER_CONFIG, keyConverter);
        return this;
    }

    public ConnectorConfig withValueConverter(String valueConverter) {
        config.put(VALUE_CONVERTER_CONFIG, valueConverter);
        return this;
    }

    public ConnectorConfig withHeaderConverter(String headerConverter) {
        config.put(HEADER_CONVERTER_CONFIG, headerConverter);
        return this;
    }

    public ConnectorConfig with(String key, Object value) {
        config.put(key, value);
        return this;
    }

    @JsonProperty("name")
    private final String name;

    @JsonProperty("config")
    private final Map<String, Object> config;

    @JsonCreator
    public ConnectorConfig(@JsonProperty("name") String name, @JsonProperty("config") Map<String, Object> config) {
        this.name = name;
        this.config = config;
    }

    @Override
    public String toString() {
        return "ConnectorConfig{" +
                "name='" + name + '\'' +
                ", config=" + config +
                '}';
    }
}
