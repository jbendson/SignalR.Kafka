using Ascentis.SignalR.Kafka.Extensions;
using Ascentis.SignalR.Kafka.IntegrationTests.Server.AuthenticationHandlers;
using Ascentis.SignalR.Kafka.IntegrationTests.Server.Hubs;
using Confluent.Kafka;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace Ascentis.SignalR.Kafka.IntegrationTests.Server;

public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        var bootstrapServers = "192.168.28.43:9092,192.168.28.51:9092,192.168.28.52:9092";
        services
            .AddSignalR()
            .AddKafka((options) =>
            {
                options.ConsumerConfig = new ConsumerConfig
                {
                    GroupId = $"{Environment.MachineName}_{Guid.NewGuid():N}",
                    BootstrapServers = bootstrapServers,
                    AutoOffsetReset = AutoOffsetReset.Latest,
                    EnableAutoCommit = true
                };
                options.ProducerConfig = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,
                    ClientId = $"{Environment.MachineName}_{Guid.NewGuid():N}"
                };
                options.KafkaTopicConfig = new KafkaTopicConfig(topicPrefix: "test", ackSpecification: new Internal.KafkaTopicSpecification());
            });
        services
            .AddAuthentication(options =>
            {
                options.DefaultScheme = TestAuthenticationHandler.SchemeName;
            })
            .AddScheme<AuthenticationSchemeOptions, TestAuthenticationHandler>(
                TestAuthenticationHandler.SchemeName,
                op => { });
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }

        app.UseRouting();
        app.UseAuthentication();
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapGet("/", async context =>
            {
                await context.Response.WriteAsync("Hello World!");
            });
            endpoints.MapHub<TestHub>("testHub");
        });
    }
}