using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Ascentis.SignalR.Kafka.IntegrationTests.Extensions;

public static class ProcessListExtensions
{
    public static void InitServers(this List<Process> servers, int[] ports)
    {
        foreach (var port in ports)
        {
            var processStartInfo = new ProcessStartInfo
            {
                WorkingDirectory = @"../../../../Ascentis.SignalR.Kafka.IntegrationTests.Server/bin/Release/net6.0/",
                FileName = @"../../../../Ascentis.SignalR.Kafka.IntegrationTests.Server/bin/Release/net6.0/Ascentis.SignalR.Kafka.IntegrationTests.Server.exe",
                UseShellExecute = false,
                CreateNoWindow = false,
                Arguments = port.ToString(),
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            var server = Process.Start(processStartInfo);

            server.OutputDataReceived += new DataReceivedEventHandler((sender, e) => Console.WriteLine($"[server-{port}-output] {e.Data}"));
            server.BeginOutputReadLine();
            server.ErrorDataReceived += new DataReceivedEventHandler((sender, e) => Console.WriteLine($"[server-{port}-error] {e.Data}"));
            server.BeginErrorReadLine();

            servers.Add(server);
        }
    }
}