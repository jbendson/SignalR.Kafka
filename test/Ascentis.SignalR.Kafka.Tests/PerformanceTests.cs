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
public class PerformanceTests
{
    private static readonly int[] _ports = new int[] { 6010 };
    private const int RpcWait = 1000;
    private const int StartupWait = 5000;
    private const int ConnectionCount = 5;
    private static List<Process> _servers = new();
    private static readonly SemaphoreSlim _testLock = new (1);
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
        foreach (var port in _ports)
        {
            var processStartInfo = new ProcessStartInfo
            {
                FileName = @"..\..\..\..\Ascentis.SignalR.Kafka.IntegrationTests.Server\bin\Debug\net6.0\Ascentis.SignalR.Kafka.IntegrationTests.Server.exe",
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
        const int messagesPerConnection = 1000;
        await PrimeServers();
        var startTime = DateTime.UtcNow;
        var tasks = new List<Task>(ConnectionCount);
        foreach (var connection in _connectionManager)
            for (var i = 0; i < messagesPerConnection; i++)
                tasks.Add(connection.InvokeAsync("SendAll", $"{_message} {i}"));

        while (_messageManager.LifetimeEnqueued() < ConnectionCount * ConnectionCount * messagesPerConnection && (DateTime.UtcNow - startTime).TotalMilliseconds < ConnectionCount * RpcWait * 10)
            await Task.Delay(10);

        await Task.WhenAll(tasks);
        Assert.AreEqual(ConnectionCount * ConnectionCount * messagesPerConnection, _messageManager.LifetimeEnqueued());
        TestContext.WriteLine($"Elapsed ms: {(DateTime.UtcNow - startTime).TotalMilliseconds}");
    }
}