using System;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;

namespace Ascentis.SignalR.Kafka;

public static class KafkaDependencyInjectionExtensions
{
    public static ISignalRServerBuilder AddKafka(this ISignalRServerBuilder signalrBuilder, Action<KafkaOptions> config)
    {
        signalrBuilder.Services.Configure(config);
        signalrBuilder.Services.AddSingleton(typeof(HubLifetimeManager<>), typeof(KafkaHubLifetimeManager<>));
        return signalrBuilder;
    }
}