package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.GenericContainer;

public class ZooKeeperContainer extends GenericContainer<ZooKeeperContainer> {
    final int zkPort = 2181;

    public ZooKeeperContainer(String repository, String tag) {
        super(repository + "/cp-zookeeper:"  + tag);
        withEnv("ZOOKEEPER_CLIENT_PORT", "" + zkPort);
        withEnv("ZOOKEEPER_TICK_TIME", "2000");
        withExposedPorts(zkPort);
//        withLogConsumer(o -> System.out.print(o.getUtf8String()));
    }

    public String getInternalConnect() {
        return getNetworkAliases().get(0) + ":" + zkPort;
    }
}
