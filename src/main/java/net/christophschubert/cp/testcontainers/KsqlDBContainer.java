package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class KsqlDBContainer extends CPTestContainer<KsqlDBContainer> {

    static final int defaultPort = 8088;

    KsqlDBContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName, bootstrap, network, defaultPort);

        withEnv("SCHEMA_REGISTRY_HOST_NAME", "ksqldb-server");
        withEnv("KSQL_BOOTSTRAP_SERVERS", getInternalBootstrap(bootstrap));
        withEnv("KSQL_LISTENERS", httpPortListener());
        withEnv("KSQL_CACHE_MAX_BYTES_BUFFERING", "0");
    }




}
