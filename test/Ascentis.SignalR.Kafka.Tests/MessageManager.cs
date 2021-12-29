using System.Collections.Concurrent;
using System.Threading;

namespace Ascentis.SignalR.Kafka.IntegrationTests;

internal class MessageManager
{
    private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> _messages = new();
    private int _internalId;

    public void EnqueueMessage(string connectionId, string message)
    {
        _messages.AddOrUpdate(connectionId, (key) =>
        {
            var queue = new ConcurrentQueue<string>();
            queue.Enqueue(message);
            Interlocked.Increment(ref _internalId);
            return queue;
        }, (key, queue) => 
        {
            queue.Enqueue(message);
            Interlocked.Increment(ref _internalId);
            return queue;
        });
    }

    public string DequeueMessage(string connectionId)
    {
        if (_messages.TryGetValue(connectionId, out var queue) && queue.TryDequeue(out var result))
            return result;

        return null;
    }

    public int LifetimeEnqueued()
    {
        return Interlocked.CompareExchange(ref _internalId, 0, 0);
    }
}