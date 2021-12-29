using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ascentis.SignalR.Kafka.Extensions;
using Ascentis.SignalR.Kafka.Internal;
using Confluent.Kafka;
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
        InitKafkaConnection(_options.ConsumerConfig, _options.ProducerConfig);
            
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
        return PublishAsync("send-all", message);
    }

    public override Task SendAllExceptAsync(string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
    {
        var message = _protocol.WriteInvocation(methodName, args, excludedConnectionIds);
        return PublishAsync("send-all", message);
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
        return PublishAsync("send-conn", connectionId, message);
    }

    public override Task SendGroupAsync(string groupName, string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        if (groupName == null)
        {
            throw new ArgumentNullException(nameof(groupName));
        }

        var message = _protocol.WriteInvocation(methodName, args);
        return PublishAsync("send-group", groupName, message);
    }

    public override Task SendGroupExceptAsync(string groupName, string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
    {
        if (groupName == null)
        {
            throw new ArgumentNullException(nameof(groupName));
        }

        var message = _protocol.WriteInvocation(methodName, args, excludedConnectionIds);
        return PublishAsync("send-group", groupName, message);
    }

    public override Task SendUserAsync(string userId, string methodName, object[] args, CancellationToken cancellationToken = default)
    {
        var message = _protocol.WriteInvocation(methodName, args);
        return PublishAsync("send-user", userId, message);
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
            publishTasks.Add(PublishAsync("send-conn", connectionId, payload));
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
                publishTasks.Add(PublishAsync("send-group", groupName, payload));
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
                publishTasks.Add(PublishAsync("send-user", userId, payload));
            }
        }

        return Task.WhenAll(publishTasks);
    }

    private async Task PublishAsync(string topic, string key, byte[] payload)
    {
        var message = new Message<string, byte[]> { Key = key, Value = payload };
        await _producer.ProduceAsync(topic, message);
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

    /// <summary>
    /// This takes <see cref="HubConnectionContext"/> because we want to remove the connection from the
    /// _connections list in OnDisconnectedAsync and still be able to remove groups with this method.
    /// </summary>
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
        await PublishAsync("group-mgmt", groupName, message);
        await ack;
    }

    /// <summary>
    /// Cleans up the Kafka connection.
    /// </summary>
    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _connectionLock.Dispose();
        _producer?.Dispose();
        _consumer?.Dispose();
    }
    
    private void InitKafkaConnection(ConsumerConfig consumerConfig, ProducerConfig producerConfig)
    {
        if (_kafkaInitialized) 
            return;

        _connectionLock.Wait();
        try
        {
            if (_kafkaInitialized) 
                return;

            var producerBuilder = new ProducerBuilder<string, byte[]>(producerConfig)
                .SetLogHandler((_, logMessage) =>
                {
                    _logger.Log(logMessage.Level.ToLogLevel(), logMessage.Message);
                });
            _producer = producerBuilder.Build();

            var consumerBuilder = new ConsumerBuilder<string, byte[]>(consumerConfig)
                .SetLogHandler((_, logMessage) =>
                {
                    _logger.Log(logMessage.Level.ToLogLevel(), logMessage.Message);
                });
            _consumer = new KafkaConsumer(consumerBuilder, _logger);
            _consumer.Subscribe(new List<string> { "send-all", "send-conn", "send-group", "send-user", "ack", "group-mgmt" });
            _consumer.StartConsuming(async (result, cancellationToken) => await ConsumeMessages(result, cancellationToken));
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

        await PublishAsync("ack", groupMessage.ServerName, _protocol.WriteAck(groupMessage.Id));
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
        await _connections[connectionId].WriteAsync(invocation.Message, cancellationToken);
    }

    private async Task SendAllInvocation(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var invocation = _protocol.ReadInvocation(consumeResult.Message.Value);
        var tasks = new List<Task>(_connections.Count);

        foreach (var connection in _connections)
        {
            if (invocation.ExcludedConnectionIds != null && invocation.ExcludedConnectionIds.Contains(connection.ConnectionId))
            {
                _logger.LogDebug("SendAll exclude connection {connectionId}", connection.ConnectionId);
                continue;
            }
            _logger.LogDebug("SendAll write connection {connectionId}", connection.ConnectionId);
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
        switch (consumeResult.Topic)
        {
            case "send-conn":
                await SendConnectionInvocation(consumeResult, cancellationToken);
                break;
            case "send-all":
                await SendAllInvocation(consumeResult, cancellationToken);
                break;
            case "send-group":
                await SendGroupInvocation(consumeResult, cancellationToken);
                break;
            case "send-user":
                await SendUserInvocation(consumeResult, cancellationToken);
                break;
            case "ack":
                AckInvocation(consumeResult);
                break;
            case "group-mgmt":
                await SendGroupManagementInvocation(consumeResult);
                break;
        }
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