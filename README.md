# signalr-kakfa

An Apache Kafka backplane for ASP.NET Core SignalR

This project is largely based off of a fork of the [SignalR Core Redis provider](https://github.com/dotnet/aspnetcore/tree/main/src/SignalR/server/StackExchangeRedis). It uses Kafka as the backplane to send SignalR messages between multiple servers, allowing for horizontal scaling of the SignalR implementation. The [Confluent Kafka dotnet client](https://github.com/confluentinc/confluent-kafka-dotnet) is leveraged for publishing and consuming messages.

## Kafka Configuration

Kafka topics are created automatically during startup if they don't yet exist. If not specified, the default topic configuration uses 10 partitions and a replication factor of 1.

Topics may also be manually created in Kafka prior to running. The following schema is used. A partitioning strategy may be designed based off the key information provided for each topic.
* **(_prefix-_)ack**: Acknowledge group management messages: Key is server unique name
* **(_prefix-_)group-mgmt**: Group management messages (add/removal of connection from group): Key is server unique name
* **(_prefix-_)send-all**: Messages intended for all connected clients: No key is used, messages will be delivered round robin to partitions
* **(_prefix-_)send-conn**: Messages intended for a specific client connection. Key is connection id
* **(_prefix-_)send-group**: Messages intended for a specific group. Key is group name
* **(_prefix-_)send-user**: Messages intended for a specific user. Key is user id

## Usage

1. Install the [Ascentis.SignalR.Kafka](https://www.nuget.org/packages/Ascentis.SignalR.Kafka/) NuGet package.
2. In ConfigureServices in Startup.cs, configure SignalR with `.AddKafka()`:

```
.AddSignalR()
.AddKafka((options) =>
{
    options.ConsumerConfig = new ConsumerConfig
    {
        GroupId = $"{Environment.MachineName}_{Guid.NewGuid():N}",
        BootstrapServers = bootstrapServers,
        AutoOffsetReset = AutoOffsetReset.Latest,
        EnableAutoCommit = true
    };
    options.ProducerConfig = new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        ClientId = $"{Environment.MachineName}_{Guid.NewGuid():N}"
    };
});
```

The configuration for producer and consumer must be specified with `options.ConsumerConfig` and `options.ProducerConfig`. A topic prefix may be configured thru the KafkaTopicConfig object to allow for multiple instances of the schema on a single Kafka deployment:

```
.AddKafka((options) =>
{
    options.KafkaTopicConfig = new KafkaTopicConfig(topicPrefix: "my-prefix");
});
```

The KafkaTopicConfig may also be used for specifying initial topic creation specifications for each topic in the schema:

```
.AddKafka((options) =>
{
    options.KafkaTopicConfig = new KafkaTopicConfig(
        ackSpecification: new KafkaTopicSpecification
        {
            ReplicationFactor = 1,
            NumPartitions = 10
        },
        groupManagementSpecification: new KafkaTopicSpecification
        {
            ReplicationFactor = 1,
            NumPartitions = 10
        });
});
```

## Performance Considerations

The Confluent Kafka client producer accumulates messages internally and sends them in batches. This supports high overall throughput, but sacrifices latency for individual message delivery. The latency and buffer sizes are configurable using Kafka ProducerConfig. See the [Confluent documentation](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html) for more information.

By default, produce is called asyncrounously and the SignalR action (Send) returns prior to message delivery to the Kafka server. This allows for the highest throughput but comes with some risk that message delivery to the server may fail silently from client perspective (exceptions will still be logged). The configuration options allow changing the behavior to syncronously await produce call completion:

```
.AddKafka((options) =>
{
    options.AwaitProduce = true;
});
```

Any exceptions during the produce operation should bubble up to the client when AwaitProduce is true. However, this configuration will dramatically reduce throughput. See the [Confluent documentation](https://docs.confluent.io/clients-confluent-kafka-dotnet/current/overview.html#producer) for more information on typical producer usage patterns.
