package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

/**
 *
 */
abstract public class CPTestContainer<SELF extends GenericContainer<SELF>> extends GenericContainer<SELF> {
    public static String getInternalBootstrap(KafkaContainer bootstrap) {
        return bootstrap.getNetworkAliases().get(0) + ":9092";
    }

    CPTestContainer(DockerImageName dockerImageName, KafkaContainer bootstrap, Network network) {
        super(dockerImageName);
        dependsOn(bootstrap);
        withNetwork(network);
    }

    CPTestContainer(ImageFromDockerfile dockerImage, KafkaContainer bootstrap, Network network) {
        super(dockerImage);

        dependsOn(bootstrap);
        withNetwork(network);
    }
}
