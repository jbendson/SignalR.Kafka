using Confluent.Kafka;
using System.ComponentModel.DataAnnotations;

namespace Ascentis.SignalR.Kafka;

public class KafkaOptions
{
    [Required]
    public ConsumerConfig ConsumerConfig { get; set; }

    [Required]
    public ProducerConfig ProducerConfig { get; set; }
}