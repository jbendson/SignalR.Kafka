using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Ascentis.SignalR.Kafka.IntegrationTests;

[TestClass]
public class IntegrationTests
{
    private static readonly int[] _ports = new int[] { 5010, 5011 };
    private const int RpcWait = 2000;
    private const int StartupWait = 10000;
    private const int GroupConnectionCount = 3;
    private readonly int ConnectionCount = 5;
    private static List<Process> _servers = new();
    private static readonly SemaphoreSlim _testLock = new (1);
    private SemaphoreSlim _lockObj;
    private ConnectionManager _connectionManager;
    private MessageManager _messageManager;
    private string _message;

    public TestContext TestContext { get; set; }

    [ClassInitialize]
    public static void ClassInitialize(TestContext _)
    {
        CleanupServers();
        InitServers();
        // wait for server startup
        Thread.Sleep(StartupWait);
    }

    [ClassCleanup]
    public static void ClassCleanup()
    {
        CleanupServers();
    }

    [TestInitialize]
    public async Task Initialize()
    {
        // run single test at a time
        _testLock.Wait();

        _connectionManager = new ConnectionManager();
        _lockObj = new SemaphoreSlim(ConnectionCount, ConnectionCount);
        _messageManager = new MessageManager();
        await InitReceiveMessages(ConnectionCount, _lockObj, _connectionManager, (string connectionId, string message) =>
        {
            try
            {
                _messageManager.EnqueueMessage(connectionId, message);
                _lockObj.Release();
            }
            catch(Exception ex)
            {
                TestContext.WriteLine(ex.Message);
                throw;
            }
        });
        _message = Guid.NewGuid().ToString();
    }

    [TestCleanup]
    public async Task Cleanup()
    {
        _testLock?.Release();
        await TestObjectCleanup();
    }

    private async Task TestObjectCleanup()
    {
        if (_connectionManager != null)
            await _connectionManager.DisposeAsync();
        _lockObj?.Dispose();
    }

    private static void InitServers()
    {
        _servers = new List<Process>();
        foreach (var port in _ports)
        {
            var processStartInfo = new ProcessStartInfo
            {
                WorkingDirectory = @"..\..\..\..\Ascentis.SignalR.Kafka.IntegrationTests.Server\bin\Debug\net6.0\",
                FileName = @"Ascentis.SignalR.Kafka.IntegrationTests.Server.exe",
                UseShellExecute = true,
                CreateNoWindow = false,
                Arguments = port.ToString()
            };

            var server = Process.Start(processStartInfo);
            if (server == null)
                Assert.Fail("server process not started");

            _servers.Add(server);
        }
    }

    private static void CleanupServers()
    {
        foreach (var server in _servers)
        {
            server.Kill();
            server.WaitForExit(10000);
            server.Dispose();
        }

        _servers = null;
    }

    private static async Task InitReceiveMessages(int connectionCount, SemaphoreSlim lockObj,
        ConnectionManager connectionManager, Action<string, string> receiveAction)
    {
        for (int i = 0; i < connectionCount; i++)
        {
            var port = _ports[i % _ports.Length];
            var connection = new HubConnectionBuilder()
                .WithUrl($"http://localhost:{port}/testHub/", options => 
                    { 
                        options.Headers = new Dictionary<string, string> { { "TestAuthHeader", $"user{i}" } };
                    })
                .Build();

            await connection.StartAsync();
            await lockObj.WaitAsync();

            connection.On("ReceiveMessage", (string message) => {
                receiveAction(connection.ConnectionId, message);
            });

            connectionManager.Add(connection, port);
        }
    }

    private static List<HubConnection> GetRandomConnections(ConnectionManager connectionManager, int count)
    {
        var connections = connectionManager.ToList();
        if (count > connections.Count)
            throw new ArgumentException("invalid count args");

        var results = new List<HubConnection>();

        var rnd = new Random();
        var ids = new List<int>();
        while (results.Count < count)
        {
            var id = rnd.Next(0, connections.Count);
            if (ids.Contains(id))
                continue;
                
            results.Add(connections[id]);
            ids.Add(id);
        }

        return results;
    }

    [TestMethod]
    public async Task SendAll_LocalSingleClient()
    {
        // test does not use common test objects
        await TestObjectCleanup();

        using var lockObj = new SemaphoreSlim(1, 1);

        var connection = new HubConnectionBuilder()
            .WithUrl($"http://localhost:{_ports.First()}/testHub/")
            .Build();

        await connection.StartAsync();
        await lockObj.WaitAsync();

        var messages = new List<string>();
        connection.On("ReceiveMessage", (string message) =>
        {
            messages.Add(message);
            lockObj.Release();
        });

        var message1 = Guid.NewGuid().ToString();
        await connection.InvokeAsync("SendAll", message1);
        await lockObj.WaitAsync(RpcWait);
        Assert.AreEqual(1, messages.Count);
        Assert.AreEqual(message1, messages.Last());

        var message2 = Guid.NewGuid().ToString();
        await connection.InvokeAsync("SendAll", message2);
        await lockObj.WaitAsync(RpcWait);
        Assert.AreEqual(2, messages.Count);
        Assert.AreEqual(message2, messages.Last());
    }

    [TestMethod]
    public async Task SendAll()
    {
        await _connectionManager.First().InvokeAsync("SendAll", _message);
            
        foreach (var _ in _connectionManager)
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in _connectionManager)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(_connectionManager.Count(), _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendConnection()
    {
        var connectionId = ConnectionCount > 1
            ? _connectionManager.Skip(1).Take(1).First().ConnectionId
            : _connectionManager.First().ConnectionId;

        await _connectionManager.First().InvokeAsync("SendConnection", connectionId, _message);
        await _lockObj.WaitAsync(RpcWait);

        Assert.AreEqual(1, _messageManager.LifetimeEnqueued());

        var receivedMessage = _messageManager.DequeueMessage(connectionId);
        if (receivedMessage == null)
            Assert.Fail($"expected _message for {connectionId}");

        Assert.AreEqual(_message, receivedMessage);
    }


    [TestMethod]
    public async Task SendConnection_NonExisting()
    {
        var connectionId = "not existing";
        await _connectionManager.First().InvokeAsync("SendConnection", connectionId, _message);
        // expect a timeout
        await _lockObj.WaitAsync(RpcWait);

        Assert.AreEqual(0, _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendAll_Except()
    {
        var connectionId = ConnectionCount > 1
            ? _connectionManager.Skip(1).Take(1).First().ConnectionId
            : _connectionManager.First().ConnectionId;

        await _connectionManager.First().InvokeAsync("SendAllExcept", new[] { connectionId }, _message);

        var includedConnections = _connectionManager.Where(x => x.ConnectionId != connectionId).ToList();
        foreach (var _ in includedConnections)
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in includedConnections)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(includedConnections.Count, _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendGroup()
    {
        var group = Guid.NewGuid().ToString();
        var groupConnections = GetRandomConnections(_connectionManager, GroupConnectionCount);
        foreach (var connection in groupConnections)
            await connection.InvokeAsync("AddGroup", group);
        await _connectionManager.First().InvokeAsync("SendGroup", group, _message);

        foreach (var _ in groupConnections)
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in groupConnections)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(groupConnections.Count, _messageManager.LifetimeEnqueued());
    }


    [TestMethod]
    public async Task SendGroup_RemoteServer()
    {
        if (_ports.Length < 2)
        {
            Assert.Inconclusive("Test requires multiple servers");
            return;
        }

        var group = Guid.NewGuid().ToString();
        var invocationConnection = _connectionManager.First();
        var remoteConnection = _connectionManager.Skip(1).Take(1).First();

        Assert.AreNotEqual(_connectionManager.GetServerPort(invocationConnection), _connectionManager.GetServerPort(remoteConnection));

        var sw = new Stopwatch();
        sw.Start();
        await invocationConnection.InvokeAsync("AddGroupConnection", group, remoteConnection.ConnectionId);
        TestContext.WriteLine($"AddGroupConnection elapsed ms: {sw.ElapsedMilliseconds}");
        sw.Restart();
        await invocationConnection.InvokeAsync("SendGroup", group, _message);
        TestContext.WriteLine($"SendGroup elapsed ms: {sw.ElapsedMilliseconds}");
        sw.Restart();
        await _lockObj.WaitAsync(RpcWait);
        TestContext.WriteLine($"SendGroup received elapsed ms: {sw.ElapsedMilliseconds}");

        var receivedMessage = _messageManager.DequeueMessage(remoteConnection.ConnectionId);
        if (receivedMessage == null)
            Assert.Fail($"expected _message for {remoteConnection.ConnectionId}");

        Assert.AreEqual(_message, receivedMessage);
        Assert.AreEqual(1, _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendGroup_LocalAndRemoteServer()
    {
        if (_ports.Length < 2)
        {
            Assert.Inconclusive("Test requires multiple servers");
            return;
        }

        var group = Guid.NewGuid().ToString();
        List<HubConnection> groupConnections = null;
        while (groupConnections == null || groupConnections.DistinctBy(x => _connectionManager.GetServerPort(x)).Count() < 2)
            groupConnections = GetRandomConnections(_connectionManager, GroupConnectionCount);

        var invocationConnection = _connectionManager.First();
        foreach (var connection in groupConnections)
            await invocationConnection.InvokeAsync("AddGroupConnection", group, connection.ConnectionId);
        await invocationConnection.InvokeAsync("SendGroup", group, _message);

        foreach (var _ in groupConnections)
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in groupConnections)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(groupConnections.Count, _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendGroup_Except()
    {
        var group = Guid.NewGuid().ToString();
        var groupConnections = GetRandomConnections(_connectionManager, GroupConnectionCount);
        foreach (var connection in groupConnections)
            await connection.InvokeAsync("AddGroup", group);

        await _connectionManager.First().InvokeAsync("SendGroupExcept", new [] { groupConnections.First().ConnectionId }, group, _message);

        var includedConnections = groupConnections.Skip(1).Take(groupConnections.Count - 1).ToList();
        foreach (var _ in includedConnections)
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in includedConnections)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(includedConnections.Count, _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendGroup_NonExisting()
    {
        var group = Guid.NewGuid().ToString();
            
        await _connectionManager.First().InvokeAsync("SendGroup", group, _message);
        await Task.Delay(RpcWait);
        Assert.AreEqual(0, _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendGroup_OthersInGroup()
    {
        var group = Guid.NewGuid().ToString();
        var groupConnections = GetRandomConnections(_connectionManager, GroupConnectionCount);
        foreach (var connection in groupConnections)
            await connection.InvokeAsync("AddGroup", group);
        await groupConnections.First().InvokeAsync("SendOthersInGroup", group, _message);

        var includedConnections = groupConnections.Skip(1).Take(groupConnections.Count - 1);
        foreach (var _ in includedConnections)
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in includedConnections)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(includedConnections.Count(), _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendGroups()
    {
        var group1 = Guid.NewGuid().ToString();
        var group2 = Guid.NewGuid().ToString();
        var groupConnections = GetRandomConnections(_connectionManager, GroupConnectionCount);
        var rnd = new Random();
        foreach (var connection in groupConnections)
        {
            var rando = rnd.Next(2);
            switch (rando)
            {
                case 0:
                    await connection.InvokeAsync("AddGroup", group1);
                    break;
                case 1:
                    await connection.InvokeAsync("AddGroup", group2);
                    break;
                default:
                    await connection.InvokeAsync("AddGroup", group1);
                    await connection.InvokeAsync("AddGroup", group2);
                    break;
            }
        }

        await _connectionManager.First().InvokeAsync("SendGroups", new[] { group1, group2 }, _message);

        foreach (var _ in groupConnections)
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in groupConnections)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(groupConnections.Count, _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendUser()
    {
        var receivingIndex = ConnectionCount > 1 ? 1 : 0;
        var connectionId = _connectionManager.Skip(receivingIndex).Take(1).First().ConnectionId;
        await _connectionManager.First().InvokeAsync("SendUser", $"user{receivingIndex}", _message);
        await _lockObj.WaitAsync(RpcWait);

        Assert.AreEqual(1, _messageManager.LifetimeEnqueued());

        var receivedMessage = _messageManager.DequeueMessage(connectionId);
        if (receivedMessage == null)
            Assert.Fail($"expected _message for {connectionId}");

        Assert.AreEqual(_message, receivedMessage);
    }

    [TestMethod]
    public async Task SendUsers()
    {
        if (ConnectionCount < 2)
            Assert.Inconclusive("Test requires multiple client connections");

        var userConnections = new List<HubConnection> { _connectionManager.First(), _connectionManager.Skip(1).Take(1).First() };

        await _connectionManager.First().InvokeAsync("SendUsers", new[] { $"user{0}", $"user{1}" }, _message);
        foreach (var _ in userConnections)
            await _lockObj.WaitAsync(RpcWait);

        Assert.AreEqual(userConnections.Count, _messageManager.LifetimeEnqueued());

        foreach (var connection in userConnections)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }
    }

    [TestMethod]
    public async Task SendGroup_Remove()
    {
        var group = Guid.NewGuid().ToString();
        var groupConnections = GetRandomConnections(_connectionManager, GroupConnectionCount);
        foreach (var connection in groupConnections)
            await connection.InvokeAsync("AddGroup", group);
        await _connectionManager.First().InvokeAsync("SendGroup", group, _message);
        foreach (var _ in groupConnections)
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in groupConnections)
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(GroupConnectionCount, _messageManager.LifetimeEnqueued());

        // remove first groupConnection and validate they don't receive any other messages for group
        await groupConnections.First().InvokeAsync("RemoveGroup", group);
        await _connectionManager.First().InvokeAsync("SendGroup", group, _message);
        foreach (var _ in groupConnections.Skip(1).Take(GroupConnectionCount - 1))
            await _lockObj.WaitAsync(RpcWait);

        foreach (var connection in groupConnections.Skip(1).Take(GroupConnectionCount - 1))
        {
            var receivedMessage = _messageManager.DequeueMessage(connection.ConnectionId);
            if (receivedMessage == null)
                Assert.Fail($"expected _message for {connection.ConnectionId}");

            Assert.AreEqual(_message, receivedMessage);
        }

        Assert.AreEqual(GroupConnectionCount * 2 - 1, _messageManager.LifetimeEnqueued());
    }

    [TestMethod]
    public async Task SendGroup_RemoveRemote()
    {
        if (_ports.Length < 2)
        {
            Assert.Inconclusive("Test requires multiple servers");
            return;
        }

        var group = Guid.NewGuid().ToString();
        var invocationConnection = _connectionManager.First();
        var remoteConnection = _connectionManager.Skip(1).Take(1).First();

        Assert.AreNotEqual(_connectionManager.GetServerPort(invocationConnection), _connectionManager.GetServerPort(remoteConnection));

        await invocationConnection.InvokeAsync("AddGroupConnection", group, remoteConnection.ConnectionId);
        await invocationConnection.InvokeAsync("SendGroup", group, _message);
        await _lockObj.WaitAsync(RpcWait);

        var receivedMessage = _messageManager.DequeueMessage(remoteConnection.ConnectionId);
        if (receivedMessage == null)
            Assert.Fail($"expected _message for {remoteConnection.ConnectionId}");

        Assert.AreEqual(_message, receivedMessage);
        Assert.AreEqual(1, _messageManager.LifetimeEnqueued());

        // remove remoteConnection and validate they don't receive any other messages for group
        await invocationConnection.InvokeAsync("RemoveGroupConnection", group, remoteConnection.ConnectionId);
        await invocationConnection.InvokeAsync("SendGroup", group, _message);
        // expect a timeout here
        await _lockObj.WaitAsync(RpcWait);

        Assert.AreEqual(1, _messageManager.LifetimeEnqueued());
    }
}