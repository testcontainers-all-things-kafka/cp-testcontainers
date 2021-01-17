package net.christophschubert.cp.testcontainers;

import org.testcontainers.containers.KafkaContainer;

import static net.christophschubert.cp.testcontainers.CPTestContainerFactory.pToEKafka;

public class KafkaContainerTools {
    static public <C extends KafkaContainer> C adjustReplicationFactors(C container, int rf) {
        //set all replications factor and min-isr settings to rf and rf-1, resp.

        container.withEnv(pToEKafka("transaction.state.log.replication.factor"), "" + rf)
        .withEnv(pToEKafka("transaction.state.log.min.isr"), "" + rf)
        .withEnv(pToEKafka("offsets.topic.replication.factor"), "" + rf);

        return container;
    }


}
