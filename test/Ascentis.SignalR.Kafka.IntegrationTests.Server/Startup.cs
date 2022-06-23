using Ascentis.SignalR.Kafka.Extensions;
using Ascentis.SignalR.Kafka.IntegrationTests.Server.AuthenticationHandlers;
using Ascentis.SignalR.Kafka.IntegrationTests.Server.Hubs;
using Confluent.Kafka;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace Ascentis.SignalR.Kafka.IntegrationTests.Server;

public class Startup
{
    public IConfiguration Configuration { get; }

    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    public void ConfigureServices(IServiceCollection services)
    {
        services
            .AddSignalR()
            .AddKafka((options) =>
            {
                options.AdminConfig = new AdminClientConfig(GetKafkaClientConfig());
                options.ConsumerConfig = new ConsumerConfig(GetKafkaClientConfig())
                {
                    GroupId = $"{Environment.MachineName}_{Guid.NewGuid():N}",
                    AutoOffsetReset = AutoOffsetReset.Latest,
                    EnableAutoCommit = true
                };
                options.ProducerConfig = new ProducerConfig(GetKafkaClientConfig());
                options.KafkaTopicConfig = new KafkaTopicConfig(
                    ackSpecification: new KafkaTopicSpecification
                    {
                        ReplicationFactor = 1,
                        NumPartitions = 10
                    },
                    groupManagementSpecification: new KafkaTopicSpecification
                    {
                        ReplicationFactor = 1,
                        NumPartitions = 10
                    });
                options.AwaitProduce = false;
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

    private ClientConfig GetKafkaClientConfig()
    {
        var bootstrapServers = Configuration.GetValue<string>("BootstrapServers");

        return new ClientConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = $"{Environment.MachineName}_{Guid.NewGuid():N}"
        };
    }
}