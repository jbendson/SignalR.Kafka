#nullable enable

using Ascentis.SignalR.Kafka;
using Ascentis.SignalR.Kafka.Internal;
using Confluent.Kafka.Admin;

namespace Ascentis.SignalR.Kafka
{
    public class KafkaTopicConfig
    {
        public string TopicPrefix { get; }
        private readonly KafkaTopics _topics;
        public TopicSpecification AckSpecification { get; set; }
        public TopicSpecification GroupManagementSpecification { get; set; }
        public TopicSpecification SendAllSpecification { get; set; }
        public TopicSpecification SendConnectionSpecification { get; set; }
        public TopicSpecification SendGroupSpecification { get; set; }
        public TopicSpecification SendUserSpecification { get; set; }

        public KafkaTopicConfig(string? topicPrefix = null, KafkaTopicSpecification? ackSpecification = null, KafkaTopicSpecification? groupManagementSpecification = null,
            KafkaTopicSpecification? sendAllSpecification = null, KafkaTopicSpecification? sendConnectionSpecification = null, KafkaTopicSpecification? sendGroupSpecification = null,
            KafkaTopicSpecification? sendUserSpecification = null)
        {
            TopicPrefix = topicPrefix ?? string.Empty;
            _topics = new KafkaTopics(TopicPrefix);
            AckSpecification = SetupTopicSpecification(ackSpecification, _topics.Ack);
            GroupManagementSpecification = SetupTopicSpecification(groupManagementSpecification, _topics.GroupManagement);
            SendAllSpecification = SetupTopicSpecification(sendAllSpecification, _topics.SendAll);
            SendConnectionSpecification = SetupTopicSpecification(sendConnectionSpecification, _topics.SendConnection);
            SendGroupSpecification = SetupTopicSpecification(sendGroupSpecification, _topics.SendGroup);
            SendUserSpecification = SetupTopicSpecification(sendUserSpecification, _topics.SendUser);
        }

        private static TopicSpecification SetupTopicSpecification(KafkaTopicSpecification? topicSpecification, string name)
        {
            return topicSpecification == null
                ? DefaultSpecification(name)
                : new TopicSpecification
                {
                    Configs = topicSpecification.Configs,
                    Name = name,
                    ReplicasAssignments = topicSpecification.ReplicasAssignments,
                    ReplicationFactor = topicSpecification.ReplicationFactor
                };
        }

        private static TopicSpecification DefaultSpecification(string name)
        {
            return new TopicSpecification
            {
                Name = name,
                ReplicationFactor = 1,
                NumPartitions = 10
            };
        }
    }
}
