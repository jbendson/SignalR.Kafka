namespace Ascentis.SignalR.Kafka.Internal;

internal class KafkaTopics
{
    public string Ack { get; }
    public string GroupManagement { get; }
    public string SendAll { get; }
    public string SendConnection { get; }
    public string SendGroup { get; }
    public string SendUser { get; }

    public KafkaTopics(string prefix)
    {
        Ack = ChannelName(prefix, "ack");
        GroupManagement = ChannelName(prefix, "group-mgmt");
        SendAll = ChannelName(prefix, "send-all");
        SendConnection = ChannelName(prefix, "send-conn");
        SendGroup = ChannelName(prefix, "send-group");
        SendUser = ChannelName(prefix, "send-user");
    }

    private static string ChannelName(string prefix, string channelNameBase)
    {
        return string.IsNullOrWhiteSpace(prefix)
            ? channelNameBase
            : $"{prefix}-{channelNameBase}";
    }
}