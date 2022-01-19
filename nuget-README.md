# SignalR.Kafka

An Apache Kafka backplane for ASP.NET Core SignalR

This project is largely based off of a fork of the [SignalR Core Redis provider](https://github.com/dotnet/aspnetcore/tree/main/src/SignalR/server/StackExchangeRedis). It uses Kafka as the backplane to send SignalR messages between multiple servers, allowing for horizontal scaling of the SignalR implementation. The [Confluent Kafka dotnet client](https://github.com/confluentinc/confluent-kafka-dotnet) is leveraged for publishing and consuming messages.
