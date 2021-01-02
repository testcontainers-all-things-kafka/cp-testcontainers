# CP testcontainers

[Testcontainer](https://www.testcontainers.org/) modules for Confluent Platform components.

So far the following components are supported:

- Schema Registry
- ksqlDB
- Connect
- Replicator running on Connect
- REST Proxy.

See the `intTest` source set for examples on how to set up containers.

## Noteworthy demos
- LocalStackIntTest shows how to setup S3 sink connector with S3 installation based on localstack.