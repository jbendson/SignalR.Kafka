#nullable enable

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using MessagePack;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace Ascentis.SignalR.Kafka.Internal;

internal class KafkaProtocol
{
    private readonly DefaultHubMessageSerializer _messageSerializer;

    public KafkaProtocol(DefaultHubMessageSerializer messageSerializer)
    {
        _messageSerializer = messageSerializer;
    }

    public byte[] WriteInvocation(string methodName, object?[] args) =>
        WriteInvocation(methodName, args, null);

    public byte[] WriteInvocation(string methodName, object?[] args, IReadOnlyList<string>? excludedConnectionIds)
    {
        // Written as a MessagePack 'arr' containing at least these items:
        // * A MessagePack 'arr' of 'str's representing the excluded ids
        // * [The output of WriteSerializedHubMessage, which is an 'arr']
        // Any additional items are discarded.

        var memoryBufferWriter = MemoryBufferWriter.Get();
        try
        {
            var writer = new MessagePackWriter(memoryBufferWriter);

            writer.WriteArrayHeader(2);
            if (excludedConnectionIds is {Count: > 0})
            {
                writer.WriteArrayHeader(excludedConnectionIds.Count);
                foreach (var id in excludedConnectionIds)
                {
                    writer.Write(id);
                }
            }
            else
            {
                writer.WriteArrayHeader(0);
            }

            WriteHubMessage(ref writer, new InvocationMessage(methodName, args));
            writer.Flush();

            return memoryBufferWriter.ToArray();
        }
        finally
        {
            MemoryBufferWriter.Return(memoryBufferWriter);
        }
    }

    public byte[] WriteGroupCommand(GroupCommand command)
    {
        // Written as a MessagePack 'arr' containing at least these items:
        // * An 'int': the Id of the command
        // * A 'str': The server name
        // * An 'int': The action (likely less than 0x7F and thus a single-byte fixnum)
        // * A 'str': The group name
        // * A 'str': The connection Id
        // Any additional items are discarded.

        var memoryBufferWriter = MemoryBufferWriter.Get();
        try
        {
            var writer = new MessagePackWriter(memoryBufferWriter);

            writer.WriteArrayHeader(5);
            writer.Write(command.Id);
            writer.Write(command.ServerName);
            writer.Write((byte) command.Action);
            writer.Write(command.GroupName);
            writer.Write(command.ConnectionId);
            writer.Flush();

            return memoryBufferWriter.ToArray();
        }
        finally
        {
            MemoryBufferWriter.Return(memoryBufferWriter);
        }
    }

    public byte[] WriteAck(int messageId)
    {
        // Written as a MessagePack 'arr' containing at least these items:
        // * An 'int': The Id of the command being acknowledged.
        // Any additional items are discarded.

        var memoryBufferWriter = MemoryBufferWriter.Get();
        try
        {
            var writer = new MessagePackWriter(memoryBufferWriter);

            writer.WriteArrayHeader(1);
            writer.Write(messageId);
            writer.Flush();

            return memoryBufferWriter.ToArray();
        }
        finally
        {
            MemoryBufferWriter.Return(memoryBufferWriter);
        }
    }

    public Invocation ReadInvocation(ReadOnlyMemory<byte> data)
    {
        // See WriteInvocation for the format
        var reader = new MessagePackReader(data);
        ValidateArraySize(ref reader, 2, "Invocation");

        // Read excluded Ids
        IReadOnlyList<string>? excludedConnectionIds = null;
        var idCount = reader.ReadArrayHeader();
        if (idCount > 0)
        {
            var ids = new string[idCount];
            for (var i = 0; i < idCount; i++)
            {
                ids[i] = reader.ReadString();
            }

            excludedConnectionIds = ids;
        }

        // Read payload
        var message = ReadSerializedHubMessage(ref reader);
        return new Invocation(message, excludedConnectionIds);
    }

    public GroupCommand ReadGroupCommand(ReadOnlyMemory<byte> data)
    {
        var reader = new MessagePackReader(data);

        // See WriteGroupCommand for format.
        ValidateArraySize(ref reader, 5, "GroupCommand");

        var id = reader.ReadInt32();
        var serverName = reader.ReadString();
        var action = (GroupAction) reader.ReadByte();
        var groupName = reader.ReadString();
        var connectionId = reader.ReadString();

        return new GroupCommand(id, serverName, action, groupName, connectionId);
    }

    public int ReadAck(ReadOnlyMemory<byte> data)
    {
        var reader = new MessagePackReader(data);

        // See WriteAck for format
        ValidateArraySize(ref reader, 1, "Ack");
        return reader.ReadInt32();
    }

    private void WriteHubMessage(ref MessagePackWriter writer, HubMessage message)
    {
        // Written as a MessagePack 'map' where the keys are the name of the protocol (as a MessagePack 'str')
        // and the values are the serialized blob (as a MessagePack 'bin').

        var serializedHubMessages = _messageSerializer.SerializeMessage(message);

        writer.WriteMapHeader(serializedHubMessages.Count);

        foreach (var serializedMessage in serializedHubMessages)
        {
            writer.Write(serializedMessage.ProtocolName);

            var isArray = MemoryMarshal.TryGetArray(serializedMessage.Serialized, out var array);
            Debug.Assert(isArray);
            writer.Write(array);
        }
    }

    public SerializedHubMessage ReadSerializedHubMessage(ref MessagePackReader reader)
    {
        var count = reader.ReadMapHeader();
        var serializations = new SerializedMessage[count];
        for (var i = 0; i < count; i++)
        {
            var protocol = reader.ReadString();
            var serialized = reader.ReadBytes()?.ToArray() ?? Array.Empty<byte>();

            serializations[i] = new SerializedMessage(protocol, serialized);
        }

        return new SerializedHubMessage(serializations);
    }

    private static void ValidateArraySize(ref MessagePackReader reader, int expectedLength, string messageType)
    {
        var length = reader.ReadArrayHeader();

        if (length < expectedLength)
        {
            throw new InvalidDataException($"Insufficient items in {messageType} array.");
        }
    }
}