using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Ascentis.SignalR.Kafka.Internal;

internal class AckHandler : IDisposable
{
    private readonly ConcurrentDictionary<int, AckInfo> _acks = new();
    private readonly Timer _timer;
    private readonly long _ackThreshold = (long) TimeSpan.FromSeconds(30).TotalMilliseconds;
    private readonly TimeSpan _ackInterval = TimeSpan.FromSeconds(5);
    private readonly object _lock = new();
    private bool _disposed;

    public AckHandler()
    {
        _timer = NonCapturingTimer.Create(state => ((AckHandler) state!).CheckAcks(), state: this,
            dueTime: _ackInterval, period: _ackInterval);
    }

    public Task CreateAck(int id)
    {
        lock (_lock)
        {
            if (_disposed)
            {
                return Task.CompletedTask;
            }

            return _acks.GetOrAdd(id, _ => new AckInfo()).Tcs.Task;
        }
    }

    public void TriggerAck(int id)
    {
        if (_acks.TryRemove(id, out var ack))
        {
            ack.Tcs.TrySetResult();
        }
    }

    private void CheckAcks()
    {
        if (_disposed)
        {
            return;
        }

        var currentTick = Environment.TickCount64;

        foreach (var pair in _acks)
        {
            var elapsed = currentTick - pair.Value.CreatedTick;
            if (elapsed > _ackThreshold)
            {
                if (_acks.TryRemove(pair.Key, out var ack))
                {
                    ack.Tcs.TrySetCanceled();
                }
            }
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _disposed = true;

            _timer.Dispose();

            foreach (var pair in _acks)
            {
                if (_acks.TryRemove(pair.Key, out var ack))
                {
                    ack.Tcs.TrySetCanceled();
                }
            }
        }
    }

    private class AckInfo
    {
        public TaskCompletionSource Tcs { get; }
        public long CreatedTick { get; }

        public AckInfo()
        {
            CreatedTick = Environment.TickCount64;
            Tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}