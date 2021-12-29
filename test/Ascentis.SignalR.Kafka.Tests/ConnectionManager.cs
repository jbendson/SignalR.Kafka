using Microsoft.AspNetCore.SignalR.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Ascentis.SignalR.Kafka.IntegrationTests;

internal class ConnectionManager : IAsyncDisposable, IEnumerable<HubConnection>
{
    private readonly List<HubConnection> _connections = new();
    private readonly List<int> _serverPorts = new();

    public void Add(HubConnection connection, int serverPort)
    {
        _connections.Add(connection);
        _serverPorts.Add(serverPort);
    }

    public int GetServerPort(HubConnection connection)
    {
        for (int i = 0; i < _connections.Count; i++)
        {
            if (_connections[i].ConnectionId == connection.ConnectionId)
                return _serverPorts[i];
        }

        return -1;
    }

    public virtual async ValueTask DisposeAsync()
    {
        foreach (var connection in _connections)
            await connection.DisposeAsync().ConfigureAwait(false);
    }

    public IEnumerator<HubConnection> GetEnumerator()
    {
        foreach (var connection in _connections)
            yield return connection;
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}