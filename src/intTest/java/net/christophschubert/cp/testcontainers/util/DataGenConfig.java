package net.christophschubert.cp.testcontainers.util;

import java.util.HashMap;


public class DataGenConfig extends ConnectorConfig{
    public DataGenConfig(String name) {
        super(name, new HashMap<>());

        with(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "io.confluent.kafka.connect.datagen.DatagenConnector");
    }

    public DataGenConfig withKafkaTopic(String topicName) {
        with("kafka.topic", topicName);
        return this;
    }

    public DataGenConfig withMaxInterval(int maxInterval) {
        with("max.interval", maxInterval);
        return this;
    }

    public DataGenConfig withSchemaString(String avroSchema) {
        with("schema.string", avroSchema);
        return this;
    }

    public DataGenConfig withSchemaFilename(String schemaFilename) {
        with("schema.filename", schemaFilename);
        return this;
    }

    public DataGenConfig withSchemaKeyfield(String keyfield) {
        with("schema.keyfield", keyfield);
        return this;
    }

    public DataGenConfig withQuickstart(String quickstart) {
        with("quickstart", quickstart);
        return this;
    }

    /**
     * Sets the number of messages for each task, or -1 for unlimited.
     *
     * @param numMessages the number of messages to send from each task.
     * @return this connector config
     */
    public DataGenConfig withIterations(int numMessages) {
        with("iterations", numMessages);
        return this;
    }
}

