# CP testcontainers

[Testcontainer](https://www.testcontainers.org/) modules for Confluent Platform components.

## Supported components

So far the following components are supported:

- Schema Registry
- ksqlDB
- Connect
- Replicator running on Connect
- REST Proxy.

## Example

The following snippet will start an Apache Kafka and a Schema Registry container on the same network
```java
final var factory = new CPTestContainerFactory();

final KafkaContainer kafka = factory.createKafka();
final SchemaRegistryContainer schemaRegistry = factory.createSchemaRegistry(kafka);
schemaRegistry.start(); //will implicitly start kafka container
```
The following snippet can then be used to configure a producer to use these:
```java
properties.put("bootstrap.servers",   kafka.getBootstrapServers());
//...
properties.put("schema.registry.url", schemaRegistry.getBaseUrl());
```

The following snippet starts a single node Kafka Connect cluster with two connectors from [Confluent Hub](https://www.confluent.io/hub/) installed:
```java
final var connect = factory.createCustomConnector(
        Set.of(
            "confluentinc/kafka-connect-s3:latest", 
            "confluentinc/kafka-connect-datagen:0.4.0"
        ), kafka);
connect.start();
```

See the `intTest` source set for examples on how to set up containers.

## Packages

Project package are hosted on jitpack for the time being. 
Add the following to your gradle file:

```groovy
allprojects {
    repositories {
        maven { url 'https://jitpack.io' }
    }
}

dependencies {
    implementation 'com.github.christophschubert:cp-testcontainers:Tag'
}
```

See the [jitpack page](https://jitpack.io/#christophschubert/cp-testcontainers) of the project for information on how to use the package with `mvn` or `sbt`.

## Noteworthy demos
- [LocalStackIntTest](https://github.com/christophschubert/cp-testcontainers/blob/master/src/intTest/java/net/christophschubert/cp/testcontainers/LocalStackIntTest.java) shows how to setup S3 sink connector with S3 installation based on localstack.