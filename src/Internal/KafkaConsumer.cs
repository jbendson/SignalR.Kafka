using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Ascentis.SignalR.Kafka.Internal;

internal class KafkaConsumer : IDisposable
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task _consumingTask;
    private readonly Action<ConsumeResult<string, byte[]>, CancellationToken> _consumeAction;
    private const int ExceptionDelay = 1000;

    public KafkaConsumer(ConsumerBuilder<string, byte[]> consumerBuilder, ILogger logger,
        Action<ConsumeResult<string, byte[]>, CancellationToken> consumeAction)
    {
        _consumer = consumerBuilder.Build();
        _cancellationTokenSource = new CancellationTokenSource();
        _consumeAction = consumeAction;
        _logger = logger;
    }

    public void Subscribe(List<string> topics)
    {
        _consumer.Subscribe(topics);
    }

    public void StartConsuming()
    {
        var cancellationToken = _cancellationTokenSource.Token;
        _consumingTask = Task.Run(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);
                    if (consumeResult == null)
                        continue;

                    _consumeAction(consumeResult, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "ConsumingTask exception");
                    await Task.Delay(ExceptionDelay);
                }
            }
        }, cancellationToken);
    }

    public void Dispose()
    {
        _logger.LogTrace("Dispose KafkaConsumer");
        if (_consumingTask != null)
        {
            _cancellationTokenSource.Cancel();
            if (_consumingTask.IsCompleted)
                _consumingTask.Dispose();
        }
        _cancellationTokenSource.Dispose();
        _consumer.Dispose();
    }
}