using Confluent.Kafka;
using System.ComponentModel.DataAnnotations;

namespace Ascentis.SignalR.Kafka;

public class KafkaOptions
{
    public AdminClientConfig AdminConfig { get; set; }

    [Required]
    public ConsumerConfig ConsumerConfig { get; set; }

    [Required]
    public ProducerConfig ProducerConfig { get; set; }

    [Required]
    public KafkaTopicConfig KafkaTopicConfig { get; set; } = new KafkaTopicConfig();

    public bool AwaitProduce { get; set; }
}