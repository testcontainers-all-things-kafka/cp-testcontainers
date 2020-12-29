package net.christophschubert.cp.testcontainers;

import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class RestProxyContainerTest {
    @Test
    public void setupRestProxyAndProduce() throws IOException, InterruptedException {
        final var containerFactory = new CPTestContainerFactory(Network.newNetwork());

        final var kafka = containerFactory.createKafka();
        kafka.start();

        final var restProxy = containerFactory.createRestProxy(kafka);
        restProxy.withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        restProxy.start();

        final HttpClient client = HttpClient.newBuilder().build();

        final var request = HttpRequest.newBuilder(URI.create(restProxy.getBaseUrl() + "/topics/test_topic"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"records\": [{\"key\": \"key-1\", \"value\":\"value-1\"}, {\"key\": \"key-1\", \"value\":\"value-1\"}]}"))
                .header("Content-Type", "application/vnd.kafka.json.v2+json")
                .build();
        final var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response.statusCode());
        Assert.assertEquals(200, response.statusCode());

        System.out.println(response.body());

    }

    @Test
    public void produceAndConsumer() {
        //TODO: implement
    }

    @Test
    public void setupRestProxyWithSchemaRegistry() {
        //TODO: implement
    }
}
