#nullable enable

using System.Collections.Generic;
using Microsoft.AspNetCore.SignalR;

namespace Ascentis.SignalR.Kafka.Internal;

internal readonly struct Invocation
{
    public IReadOnlyList<string>? ExcludedConnectionIds { get; }

    public SerializedHubMessage Message { get; }

    public Invocation(SerializedHubMessage message, IReadOnlyList<string>? excludedConnectionIds)
    {
        Message = message;
        ExcludedConnectionIds = excludedConnectionIds;
    }
}