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
    private const int ExceptionDelay = 1000;

    public KafkaConsumer(ConsumerBuilder<string, byte[]> consumerBuilder, ILogger logger)
    {
        _logger = logger;
        _consumer = consumerBuilder.Build();
        _cancellationTokenSource = new CancellationTokenSource();
    }

    public void Subscribe(List<string> topics)
    {
        _consumer.Subscribe(topics);
    }

    public void StartConsuming(Func<ConsumeResult<string, byte[]>, CancellationToken, Task> consumeAction)
    {
        var cancellationToken = _cancellationTokenSource.Token;
        _consumingTask = Task.Run(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(cancellationToken);
                    if (result == null)
                        continue;

                    await consumeAction(result, cancellationToken);
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
            _consumingTask.Dispose();
        }
        _cancellationTokenSource.Dispose();
        _consumer.Dispose();
    }
}