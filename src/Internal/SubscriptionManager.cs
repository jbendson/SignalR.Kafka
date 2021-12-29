// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace Ascentis.SignalR.Kafka.Internal;

internal class SubscriptionManager
{
    private readonly ConcurrentDictionary<string, HubConnectionStore> _subscriptions =
        new(StringComparer.Ordinal);

    private readonly SemaphoreSlim _lock = new(1, 1);

    public Task<HubConnectionStore> GetSubscriptionAsync(string id)
    {
        var result = _subscriptions.TryGetValue(id, out var subscription) ? subscription : null;
        return Task.FromResult(result);
    }

    public async Task AddSubscriptionAsync(string id, HubConnectionContext connection)
    {
        await _lock.WaitAsync();

        try
        {
            var subscription = _subscriptions.GetOrAdd(id, _ => new HubConnectionStore());

            subscription.Add(connection);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task RemoveSubscriptionAsync(string id, HubConnectionContext connection)
    {
        await _lock.WaitAsync();

        try
        {
            if (!_subscriptions.TryGetValue(id, out var subscription))
            {
                return;
            }

            subscription.Remove(connection);

            if (subscription.Count == 0)
            {
                _subscriptions.TryRemove(id, out _);
            }
        }
        finally
        {
            _lock.Release();
        }
    }
}