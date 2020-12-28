package net.christophschubert.cp.testcontainers;


import net.christophschubert.cp.testcontainers.util.ConnectClient;
import net.christophschubert.cp.testcontainers.util.ConnectorConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
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
import java.util.Properties;
import java.util.Set;

// test to start an S3 connector with LocalStack (https://www.testcontainers.org/modules/localstack/)
public class LocalStackIntTest {
    @Test
    public void customConnectorTest() throws IOException, InterruptedException {
        final Network network = Network.newNetwork();
        final CPTestContainerFactory factory = new CPTestContainerFactory(network);

        //set up localstack/S3
        DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:0.11.3");


        LocalStackContainer localstack = new LocalStackContainer(localstackImage)
                .withServices(LocalStackContainer.Service.S3)
                .withNetwork(network)
                .withNetworkAliases("localstack");

        localstack.start();


        final var kafka = factory.createKafka();
        kafka.start();
        final var connect = factory.createCustomConnector(Set.of("confluentinc/kafka-connect-s3:latest", "confluentinc/kafka-connect-datagen:0.4.0"), kafka);
        connect.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        connect.withEnv("AWS_ACCESS_KEY", localstack.getAccessKey());
        connect.withEnv("AWS_SECRET_KEY", localstack.getSecretKey());
        connect.start();


        final var numberMessages = 100;
        final var topicName = "datagen";

        final var dataGenConfig = ConnectorConfig.source("datagen", "io.confluent.kafka.connect.datagen.DatagenConnector")
                .with("kafka.topic", topicName)
                .with("quickstart", "inventory")
                .with("iterations", numberMessages) // #msg per task
                .with("value.converter.schemas.enable", "false");

        final ConnectClient connectClient = new ConnectClient(connect.getBaseUrl());
        connectClient.startConnector(dataGenConfig);

        final var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        final Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(topicName));



        final var bucketName = "test-bucket";

        var storeUrl = localstack.getEndpointConfiguration(LocalStackContainer.Service.S3);
        System.out.println(storeUrl);

        S3ClientBuilder builder = S3Client.builder().endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3));
        builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())));
        builder.region(Region.of(localstack.getRegion()));
        S3Client s3client = builder.build();

        s3client.createBucket(builder1 -> builder1.bucket(bucketName));

        final var localstackEdgePort = 4566; //the magic localstack edge port, see https://github.com/localstack/localstack
        System.out.println("StoreURL: " + storeUrl.getServiceEndpoint());
        System.out.println("Mapped port " + localstack.getMappedPort(localstackEdgePort));


        final var s3SinkConfig = ConnectorConfig.source("s3sink", "io.confluent.connect.s3.S3SinkConnector")
                .with("format.class", "io.confluent.connect.s3.format.json.JsonFormat")
                .with("flush.size", 1)
                .withTopics(List.of(topicName))
                .with("s3.bucket.name", bucketName)
                .with("storage.class", "io.confluent.connect.s3.storage.S3Storage")
                // 4566 is special 'edge port' of localstack
                .with("store.url", "http://localstack:4566");//storeUrl.getServiceEndpoint());
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
            System.out.println(String.format("%s -> %s", key, object));
            ++msgCount;
        }
        Assert.assertEquals(numberMessages, msgCount);
    }

}
