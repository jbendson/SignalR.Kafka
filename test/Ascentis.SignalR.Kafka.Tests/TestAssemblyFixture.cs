using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Testcontainers.Kafka;

namespace Ascentis.SignalR.Kafka.IntegrationTests;

[TestClass]
public static class TestAssemblyFixture
{
    private static KafkaContainer _kafkaContainer;

    [AssemblyInitialize]
    public static async Task AssemblyInitialize(TestContext context)
    {
        _kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:6.2.10")
            .WithPortBinding(9092)
            .Build();
        await _kafkaContainer.StartAsync(context.CancellationToken);
    }

    [AssemblyCleanup]
    public static async Task AssemblyCleanup()
    {
        if (_kafkaContainer is not null)
            await _kafkaContainer.DisposeAsync();
    }
}
