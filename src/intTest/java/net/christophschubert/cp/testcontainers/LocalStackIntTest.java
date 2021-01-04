package net.christophschubert.cp.testcontainers;

import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.ConnectorConfig;
import net.christophschubert.cp.testcontainers.util.DataGenConfig;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

// test to start an S3 connector with LocalStack (https://www.testcontainers.org/modules/localstack/)
public class LocalStackIntTest {
    @Test
    public void s3sinkConnectorTest() throws IOException, InterruptedException, ExecutionException {
        final Network network = Network.newNetwork(); //explicitly set network so that localstack container can live on the same network
        final CPTestContainerFactory factory = new CPTestContainerFactory(network);

        final LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.11.3"))
                .withServices(LocalStackContainer.Service.S3)
                .withNetwork(network)
                .withNetworkAliases("localstack");

        final var kafka = factory.createKafka();
        final var connect = factory.createCustomConnector(Set.of("confluentinc/kafka-connect-s3:latest", "confluentinc/kafka-connect-datagen:0.4.0"), kafka);
        connect.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        connect.withEnv("AWS_ACCESS_KEY", localstack.getAccessKey());
        connect.withEnv("AWS_SECRET_KEY", localstack.getSecretKey());

        Startables.deepStart(List.of(connect, localstack)).get();


        final var numberMessages = 30;
        final var topicName = "datagen";

        final var dataGenConfig = new DataGenConfig("datagen")
                .withKafkaTopic(topicName)
                .withQuickstart("inventory")
                .withIterations(numberMessages)
                .with("value.converter.schemas.enable", false);

        final ConnectClient connectClient = new ConnectClient(connect.getBaseUrl());
        connectClient.startConnector(dataGenConfig);

        final S3ClientBuilder builder = S3Client.builder().endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3));
        builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())));
        builder.region(Region.of(localstack.getRegion()));
        final S3Client s3client = builder.build();

        final var bucketName = "test-bucket";
        s3client.createBucket(builder1 -> builder1.bucket(bucketName));

        final var localstackEdgePort = 4566; //the magic localstack edge port, see https://github.com/localstack/localstack

        final var s3SinkConfig = ConnectorConfig.source("s3sink", "io.confluent.connect.s3.S3SinkConnector")
                .with("format.class", "io.confluent.connect.s3.format.json.JsonFormat")
                .with("flush.size", 1)
                .withTopics(List.of(topicName))
                .with("s3.bucket.name", bucketName)
                .with("storage.class", "io.confluent.connect.s3.storage.S3Storage")
                .with("store.url", "http://localstack:" + localstackEdgePort);
        connectClient.startConnector(s3SinkConfig);

        System.out.println("S3 sink connector started");

        // give async replication time to catch up.
        Thread.sleep(Duration.ofSeconds(30).toMillis());
        System.out.println(s3client.listBuckets().buckets());
        final var listObjectsResponse = s3client.listObjects(builder1 -> builder1.bucket(bucketName).build());
        var msgCount = 0;
        for (S3Object s3Object : listObjectsResponse.contents()) {
            final var key = s3Object.key();
            Assert.assertTrue(key.startsWith("topics/" + topicName));
            final var object = s3client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build(), ResponseTransformer.toBytes()).asUtf8String();
            System.out.printf("%s -> %s", key, object);
            ++msgCount;
        }
        Assert.assertEquals(numberMessages, msgCount);
    }

}
