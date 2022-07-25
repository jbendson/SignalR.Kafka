using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ascentis.SignalR.Kafka.IntegrationTests.Extensions;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Ascentis.SignalR.Kafka.IntegrationTests;

[TestClass]
public class PerformanceTests
{
    private static readonly int[] _ports = new int[] { 6010, 6011 };
    private const int RpcWait = 1000;
    private const int StartupWait = 5000;
    private static List<Process> _servers = new();
    private static readonly SemaphoreSlim _testLock = new (1);
    private readonly int ConnectionCount = 6;
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
        _messageManager = new MessageManager();
        await InitReceiveMessages(ConnectionCount, _connectionManager, (string connectionId, string message) =>
        {
            try
            {
                _messageManager.EnqueueMessage(connectionId, message);
            }
            catch(Exception ex)
            {
                TestContext.WriteLine(ex.Message);
                throw;
            }
        });
        _message = Guid.NewGuid().ToString();
    }

    private async Task PrimeServers()
    {
        var tasks = new List<Task>(ConnectionCount);
        foreach (var connection in _connectionManager)
            tasks.Add(connection.InvokeAsync("SendAll", _message));

        await Task.WhenAll(tasks);
        var startTime = DateTime.UtcNow;
        while (_messageManager.LifetimeEnqueued() < ConnectionCount * ConnectionCount && (DateTime.UtcNow - startTime).TotalMilliseconds < RpcWait * ConnectionCount)
            await Task.Delay(10);

        _messageManager.Reset();
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
    }

    private static void InitServers()
    {
        _servers = new List<Process>();
        _servers.InitServers(_ports);

        foreach (var server in _servers)
        {
            if (server == null)
                Assert.Fail("server process not started");
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

    private static async Task InitReceiveMessages(int connectionCount, ConnectionManager connectionManager, Action<string, string> receiveAction)
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

            connection.On("ReceiveMessage", (string message) => {
                receiveAction(connection.ConnectionId, message);
            });

            connectionManager.Add(connection, port);
        }
    }

    [TestMethod]
    public async Task SendAll()
    {
        const int messagesPerConnection = 10000;
        await PrimeServers();
        var startTime = DateTime.UtcNow;
        var tasks = new List<Task>
        {
            CheckLifetimeEnqueued(ConnectionCount * ConnectionCount * messagesPerConnection)
        };
        foreach (var connection in _connectionManager)
            for (var i = 0; i < messagesPerConnection; i++)
                tasks.Add(connection.InvokeAsync("SendAll", $"{_message} {i}"));

        await Task.WhenAll(tasks);
        TestContext.WriteLine($"Received messages/sec: {_messageManager.LifetimeEnqueued() / (DateTime.UtcNow - startTime).TotalSeconds}");
        Assert.AreEqual(ConnectionCount * ConnectionCount * messagesPerConnection, _messageManager.LifetimeEnqueued());
        TestContext.WriteLine($"Elapsed ms: {(DateTime.UtcNow - startTime).TotalMilliseconds}");
    }

    [TestMethod]
    public async Task SendConnection_Pair()
    {
        if (ConnectionCount < 2)
            Assert.Inconclusive("Test requires 2 connections");

        const int messagesPerConnection = 100000;
        await PrimeServers();
        var startTime = DateTime.UtcNow;
        var tasks = new List<Task>();
        var invocationConnection = _connectionManager.First();
        var receivingConnection = _connectionManager.Skip(1).Take(1).First();
        tasks.Add(CheckLifetimeEnqueued(messagesPerConnection));
        for (var i = 0; i < messagesPerConnection; i++)
            tasks.Add(invocationConnection.InvokeAsync("SendConnection", receivingConnection.ConnectionId, $"{_message} {i}"));

        await Task.WhenAll(tasks);
        TestContext.WriteLine($"Sent/Received messages/sec: {_messageManager.LifetimeEnqueued() / (DateTime.UtcNow - startTime).TotalSeconds}");
        Assert.AreEqual(messagesPerConnection, _messageManager.LifetimeEnqueued());
        TestContext.WriteLine($"Elapsed ms: {(DateTime.UtcNow - startTime).TotalMilliseconds}");
    }

    [TestMethod]
    public async Task SendConnection_MultiplePair()
    {
        if (ConnectionCount < 4)
            Assert.Inconclusive("Test requires 2 connections");

        const int messagesPerConnection = 10000;
        int pairs = ConnectionCount / 2;
        
        await PrimeServers();
        var startTime = DateTime.UtcNow;
        var tasks = new List<Task>
        {
            CheckLifetimeEnqueued(messagesPerConnection * pairs * 2)
        };
        for (var i = 0; i < pairs; i++)
        {
            var connectionIndex = i * 2;
            var connection1 = _connectionManager.Skip(connectionIndex).Take(1).First();
            var connection2 = _connectionManager.Skip(connectionIndex + 1).Take(1).First();
            for (var j = 0; j < messagesPerConnection; j++)
            {
                tasks.Add(connection1.InvokeAsync("SendConnection", connection2.ConnectionId, $"{_message} 1 {j}"));
                tasks.Add(connection2.InvokeAsync("SendConnection", connection1.ConnectionId, $"{_message} 2 {j}"));
            }
        }

        await Task.WhenAll(tasks);
        TestContext.WriteLine($"Sent/Received messages/sec: {_messageManager.LifetimeEnqueued() / (DateTime.UtcNow - startTime).TotalSeconds}");
        Assert.AreEqual(messagesPerConnection * pairs * 2, _messageManager.LifetimeEnqueued());
        TestContext.WriteLine($"Elapsed ms: {(DateTime.UtcNow - startTime).TotalMilliseconds}");
    }

    private async Task CheckLifetimeEnqueued(int expectedMessages)
    {
        var logTime = DateTime.UtcNow;
        var startTime = DateTime.UtcNow;
        
        while (_messageManager.LifetimeEnqueued() < expectedMessages && (DateTime.UtcNow - startTime).TotalMilliseconds < ConnectionCount * RpcWait * 10)
        {
            if (DateTime.UtcNow - logTime >= TimeSpan.FromMilliseconds(1000))
            {
                TestContext.WriteLine($"messages/sec: {_messageManager.LifetimeEnqueued() / (DateTime.UtcNow - startTime).TotalSeconds}");
                logTime = DateTime.UtcNow;
            }

            await Task.Delay(10);
        }
    }
}