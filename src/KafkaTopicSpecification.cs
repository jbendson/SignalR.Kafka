using System.Collections.Generic;

namespace Ascentis.SignalR.Kafka
{
    public class KafkaTopicSpecification
    {
        public Dictionary<string, string> Configs { get; set; }
        public int NumPartitions { get; set; }
        public Dictionary<int, List<int>> ReplicasAssignments { get; set; }
        public short ReplicationFactor { get; set; }
    }
}
