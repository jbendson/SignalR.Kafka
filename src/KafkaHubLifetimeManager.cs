using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ascentis.SignalR.Kafka.Extensions;
using Ascentis.SignalR.Kafka.Internal;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ascentis.SignalR.Kafka;

public class KafkaHubLifetimeManager<THub> : HubLifetimeManager<THub>, IDisposable where THub : Hub
{
    private readonly HubConnectionStore _connections = new();
    private readonly SubscriptionManager _groups = new();
    private readonly SubscriptionManager _users = new();
    private readonly ILogger _logger;
    private readonly string _serverName = GenerateServerName();
    private readonly KafkaProtocol _protocol;
    private readonly SemaphoreSlim _connectionLock = new(1);
    private readonly KafkaOptions _options;
    private readonly KafkaTopics _topics;
    private readonly bool _awaitProduce;
    private IProducer<string, byte[]> _producer;
    private KafkaConsumer _consumer;
    private bool _kafkaInitialized;
    private readonly AckHandler _ackHandler;
    private int _internalId;

    public KafkaHubLifetimeManager(ILogger<KafkaHubLifetimeManager<THub>> logger, IOptions<KafkaOptions> options, IHubProtocolResolver hubProtocolResolver)
        : this(logger, options, hubProtocolResolver, null, null)
    {
    }

    public KafkaHubLifetimeManager(ILogger<KafkaHubLifetimeManager<THub>> logger, IOptions<KafkaOptions> options, IHubProtocolResolver hubProtocolResolver,
        IOptions<HubOptions> globalHubOptions, IOptions<HubOptions<THub>> hubOptions)
    {
        _logger = logger;
        _ackHandler = new AckHandler();
        _options = options.Value;
        _topics = new KafkaTopics(_options.KafkaTopicConfig.TopicPrefix);
        _awaitProduce = _options.AwaitProduce;

        InitKafkaConnection(_options);
            
        if (globalHubOptions != null && hubOptions != null)
        {
            _protocol = new KafkaProtocol(new DefaultHubMessageSerializer(hubProtocolResolver, globalHubOptions.Value.SupportedProtocols, hubOptions.Value.SupportedProtocols));
        }
        else
        {
            var supportedProtocols = hubProtocolResolver.AllProtocols.Select(p => p.Name).ToList();
            _protocol = new KafkaProtocol(new DefaultHubMessageSerializer(hubProtocolResolver, supportedProtocols, null));
        }
    }
        
    public override async Task OnConnectedAsync(HubConnectionContext connection)
    {
        var feature = new KafkaFeature();
        connection.Features.Set<IKafkaFeature>(feature);
        _connections.Add(connection);
        if (!string.IsNullOrEmpty(connection.UserIdentifier))
        {
            await _users.AddSubscriptionAsync(connection.UserIdentifier, connection);
        }
    }
        
    public override Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        _connections.Remove(connection);
        var tasks = new List<Task>();
        var feature = connection.Features.Get<IKafkaFeature>()!;
        var groupNames = feature.Groups;

        if (groupNames != null)
        {
            foreach (var group in groupNames.ToArray())
            {
                tasks.Add(RemoveGroupAsyncCore(connection, group));
            }
        }

        if (!string.IsNullOrEmpty(connection.UserIdentifier))
        {
            tasks.Add(_users.RemoveSubscriptionAsync(connection.UserIdentifier, connection));
        }

        return Task.WhenAll(tasks);
    }

    public override Task SendAllAsync(string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        var message = _protocol.WriteInvocation(methodName, args);
        return PublishAsync(_topics.SendAll, message);
    }

    public override Task SendAllExceptAsync(string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
    {
        var message = _protocol.WriteInvocation(methodName, args, excludedConnectionIds);
        return PublishAsync(_topics.SendAll, message);
    }
        
    public override Task SendConnectionAsync(string connectionId, string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        if (connectionId == null)
        {
            throw new ArgumentNullException(nameof(connectionId));
        }

        var connection = _connections[connectionId];
        if (connection != null)
        {
            return connection.WriteAsync(new InvocationMessage(methodName, args), cancellationToken).AsTask();
        }

        var message = _protocol.WriteInvocation(methodName, args);
        return PublishAsync(_topics.SendConnection, connectionId, message);
    }

    public override Task SendGroupAsync(string groupName, string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        if (groupName == null)
        {
            throw new ArgumentNullException(nameof(groupName));
        }

        var message = _protocol.WriteInvocation(methodName, args);
        return PublishAsync(_topics.SendGroup, groupName, message);
    }

    public override Task SendGroupExceptAsync(string groupName, string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
    {
        if (groupName == null)
        {
            throw new ArgumentNullException(nameof(groupName));
        }

        var message = _protocol.WriteInvocation(methodName, args, excludedConnectionIds);
        return PublishAsync(_topics.SendGroup, groupName, message);
    }

    public override Task SendUserAsync(string userId, string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        var message = _protocol.WriteInvocation(methodName, args);
        return PublishAsync(_topics.SendUser, userId, message);
    }
        
    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (connectionId == null)
        {
            throw new ArgumentNullException(nameof(connectionId));
        }

        if (groupName == null)
        {
            throw new ArgumentNullException(nameof(groupName));
        }

        var connection = _connections[connectionId];
        return connection != null 
            ? AddGroupAsyncCore(connection, groupName)
            : SendGroupActionAndWaitForAck(connectionId, groupName, GroupAction.Add);
    }
        
    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (connectionId == null)
        {
            throw new ArgumentNullException(nameof(connectionId));
        }

        if (groupName == null)
        {
            throw new ArgumentNullException(nameof(groupName));
        }

        var connection = _connections[connectionId];
        return connection != null
            ? RemoveGroupAsyncCore(connection, groupName)
            : SendGroupActionAndWaitForAck(connectionId, groupName, GroupAction.Remove);
    }
        
    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        if (connectionIds == null)
        {
            throw new ArgumentNullException(nameof(connectionIds));
        }

        var publishTasks = new List<Task>(connectionIds.Count);
        var payload = _protocol.WriteInvocation(methodName, args);

        foreach (var connectionId in connectionIds)
        {
            publishTasks.Add(PublishAsync(_topics.SendConnection, connectionId, payload));
        }

        return Task.WhenAll(publishTasks);
    }
        
    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        if (groupNames == null)
        {
            throw new ArgumentNullException(nameof(groupNames));
        }
        var publishTasks = new List<Task>(groupNames.Count);
        var payload = _protocol.WriteInvocation(methodName, args);

        foreach (var groupName in groupNames)
        {
            if (!string.IsNullOrEmpty(groupName))
            {
                publishTasks.Add(PublishAsync(_topics.SendGroup, groupName, payload));
            }
        }

        return Task.WhenAll(publishTasks);
    }
        
    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        if (userIds.Count <= 0) 
            return Task.CompletedTask;

        var payload = _protocol.WriteInvocation(methodName, args);
        var publishTasks = new List<Task>(userIds.Count);
        foreach (var userId in userIds)
        {
            if (!string.IsNullOrEmpty(userId))
            {
                publishTasks.Add(PublishAsync(_topics.SendUser, userId, payload));
            }
        }

        return Task.WhenAll(publishTasks);
    }

    private async Task PublishAsync(string topic, string key, byte[] payload)
    {
        var message = new Message<string, byte[]> { Key = key, Value = payload };
        if (_awaitProduce)
            await _producer.ProduceAsync(topic, message);
        else
            _producer.Produce(topic, message, PublishDeliveryHandler);
    }

    private void PublishDeliveryHandler(DeliveryReport<string, byte[]> deliveryReport)
    {
        if (deliveryReport.Error.IsError)
            _logger.Log(LogLevel.Error, "Delivery error: {reason}", deliveryReport.Error.Reason);
    }

    private Task PublishAsync(string topic, byte[] payload)
    {
        return PublishAsync(topic, null, payload);
    }

    private Task AddGroupAsyncCore(HubConnectionContext connection, string groupName)
    {
        var feature = connection.Features.Get<IKafkaFeature>()!;
        var groupNames = feature.Groups;

        lock (groupNames)
        {
            if (!groupNames.Add(groupName))
            {
                return Task.CompletedTask;
            }
        }
            
        return _groups.AddSubscriptionAsync(groupName, connection);
    }

    private async Task RemoveGroupAsyncCore(HubConnectionContext connection, string groupName)
    {
        await _groups.RemoveSubscriptionAsync(groupName, connection);

        var feature = connection.Features.Get<IKafkaFeature>()!;
        var groupNames = feature.Groups;
        if (groupNames != null)
        {
            lock (groupNames)
            {
                groupNames.Remove(groupName);
            }
        }
    }

    private async Task SendGroupActionAndWaitForAck(string connectionId, string groupName, GroupAction action)
    {
        var id = Interlocked.Increment(ref _internalId);
        var ack = _ackHandler.CreateAck(id);
        // Send Add/Remove Group to other servers and wait for an ack or timeout
        var message = _protocol.WriteGroupCommand(new GroupCommand(id, _serverName, action, groupName, connectionId));
        await PublishAsync(_topics.GroupManagement, groupName, message);
        await ack;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _connectionLock.Dispose();
        _producer?.Dispose();
        _consumer?.Dispose();
    }

    private async Task InitTopics(string bootstrapServers, KafkaTopicConfig kafkaTopicConfig)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        var topics = new List<TopicSpecification>()
        {
            kafkaTopicConfig.AckSpecification,
            kafkaTopicConfig.GroupManagementSpecification,
            kafkaTopicConfig.SendAllSpecification,
            kafkaTopicConfig.SendConnectionSpecification,
            kafkaTopicConfig.SendGroupSpecification,
            kafkaTopicConfig.SendUserSpecification
        };

        try
        {
            await adminClient.CreateTopicsAsync(topics);
            _logger.LogInformation("Created topics: {topics}", string.Join(", ", topics.Select(x => x.Name).ToArray()));
        }
        catch (CreateTopicsException e)
        {
            foreach (var result in e.Results)
                _logger.LogDebug("An error occurred creating topic {topic}: {reason}", result.Topic, result.Error.Reason);
        }
    }

    private void InitKafkaConnection(KafkaOptions options)
    {
        var consumerConfig = options.ConsumerConfig;
        var producerConfig = options.ProducerConfig;

        if (_kafkaInitialized) 
            return;

        _connectionLock.Wait();
        try
        {
            if (_kafkaInitialized) 
                return;

            InitTopics(producerConfig.BootstrapServers, options.KafkaTopicConfig).GetAwaiter().GetResult();
            var producerBuilder = new ProducerBuilder<string, byte[]>(producerConfig)
                .SetLogHandler((_, logMessage) =>
                {
                    _logger.Log(logMessage.Level.ToLogLevel(), "Kafka Log: {message}", logMessage.Message);
                });
            _producer = producerBuilder.Build();

            var consumerBuilder = new ConsumerBuilder<string, byte[]>(consumerConfig)
                .SetLogHandler((_, logMessage) =>
                {
                    _logger.Log(logMessage.Level.ToLogLevel(), "Kafka Log: {message}", logMessage.Message);
                });
            _consumer = new KafkaConsumer(consumerBuilder, _logger, async (consumeResult, cancellationToken) => await ConsumeMessages(consumeResult, cancellationToken));
            var topics = new List<string> { _topics.Ack, _topics.GroupManagement, _topics.SendAll, _topics.SendConnection, _topics.SendGroup, _topics.SendUser };
            _consumer.Subscribe(topics);
            _consumer.StartConsuming();
            _logger.LogInformation("Consuming topics: {topics}", string.Join(", ", topics));
            _kafkaInitialized = true;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task SendGroupManagementInvocation(ConsumeResult<string, byte[]> consumeResult)
    {
        var groupMessage = _protocol.ReadGroupCommand(consumeResult.Message.Value);

        var connection = _connections[groupMessage.ConnectionId];
        if (connection == null)
        {
            // user not on this server
            return;
        }

        switch (groupMessage.Action)
        {
            case GroupAction.Remove:
                await RemoveGroupAsyncCore(connection, groupMessage.GroupName);
                break;
            case GroupAction.Add:
                await AddGroupAsyncCore(connection, groupMessage.GroupName);
                break;
        }

        await PublishAsync(_topics.Ack, groupMessage.ServerName, _protocol.WriteAck(groupMessage.Id));
    }

    private void AckInvocation(ConsumeResult<string, byte[]> consumeResult)
    {
        if (!consumeResult.Message.Key.Equals(_serverName, StringComparison.Ordinal))
            return;

        var ackId = _protocol.ReadAck(consumeResult.Message.Value);
        _ackHandler.TriggerAck(ackId);
    }

    private async Task SendConnectionInvocation(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var invocation = _protocol.ReadInvocation(consumeResult.Message.Value);
        var connectionId = consumeResult.Message.Key;
        var connection = _connections[connectionId];
        if (connection == null)
        {
            _logger.LogDebug("SendConnectionInvocation connection not found {connectionId}", connectionId);
            return;
        }

        _logger.LogDebug("SendConnectionInvocation write connection {connectionId}", connectionId);
        await connection.WriteAsync(invocation.Message, cancellationToken);
    }

    private async Task SendAllInvocation(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var invocation = _protocol.ReadInvocation(consumeResult.Message.Value);
        var tasks = new List<Task>(_connections.Count);

        foreach (var connection in _connections)
        {
            if (invocation.ExcludedConnectionIds != null && invocation.ExcludedConnectionIds.Contains(connection.ConnectionId))
            {
                _logger.LogDebug("SendAllInvocation exclude connection {connectionId}", connection.ConnectionId);
                continue;
            }
            _logger.LogDebug("SendAllInvocation write connection {connectionId}", connection.ConnectionId);
            tasks.Add(connection.WriteAsync(invocation.Message, cancellationToken).AsTask());
        }

        await Task.WhenAll(tasks);
    }

    private async Task SendGroupInvocation(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var invocation = _protocol.ReadInvocation(consumeResult.Message.Value);
        var groupName = consumeResult.Message.Key;
        var connections = await _groups.GetSubscriptionAsync(groupName);
        if (connections == null)
            return;

        var tasks = new List<Task>(connections.Count);

        foreach (var connection in connections)
        {
            if (invocation.ExcludedConnectionIds == null || !invocation.ExcludedConnectionIds.Contains(connection.ConnectionId))
            {
                tasks.Add(connection.WriteAsync(invocation.Message, cancellationToken).AsTask());
            }
        }

        await Task.WhenAll(tasks);
    }

    private async Task SendUserInvocation(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var invocation = _protocol.ReadInvocation(consumeResult.Message.Value);
        var userIdentifier = consumeResult.Message.Key;
        var connections = await _users.GetSubscriptionAsync(userIdentifier);
        if (connections == null)
            return;

        var tasks = new List<Task>(connections.Count);

        foreach (var connection in connections)
        {
            tasks.Add(connection.WriteAsync(invocation.Message, cancellationToken).AsTask());
        }

        await Task.WhenAll(tasks);
    }

    private async Task ConsumeMessages(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var topic = consumeResult.Topic;

        if (topic == _topics.SendConnection)
            SendConnectionInvocation(consumeResult, cancellationToken).FireAndForget(_logger);
        else if (topic == _topics.SendAll)
            SendAllInvocation(consumeResult, cancellationToken).FireAndForget(_logger);
        else if (topic == _topics.SendGroup)
            SendGroupInvocation(consumeResult, cancellationToken).FireAndForget(_logger);
        else if (topic == _topics.SendUser)
            SendUserInvocation(consumeResult, cancellationToken).FireAndForget(_logger);
        else if (topic == _topics.Ack)
            AckInvocation(consumeResult);
        else if (topic == _topics.GroupManagement)
            // await group management to ensure group actions processed in order consumed
            await SendGroupManagementInvocation(consumeResult);
        else 
            _logger.LogError("Consuming unexpected topic: {topic}", topic);
    }

    private static string GenerateServerName()
    {
        return $"{Environment.MachineName}_{Guid.NewGuid():N}";
    }

    private interface IKafkaFeature
    {
        HashSet<string> Groups { get; }
    }

    private class KafkaFeature : IKafkaFeature
    {
        public HashSet<string> Groups { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    }
}