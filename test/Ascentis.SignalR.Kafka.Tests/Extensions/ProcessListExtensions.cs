using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Ascentis.SignalR.Kafka.IntegrationTests.Extensions;

public static class ProcessListExtensions
{
    public static void InitServers(this List<Process> servers, int[] ports)
    {
        var currentDirectory = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
        var workingDirectory = Path.Combine(new string[] { currentDirectory, "../../../../Ascentis.SignalR.Kafka.IntegrationTests.Server/bin/Release/net6.0/" });
        var serverPath = Path.Combine(new string[] { currentDirectory, "../../../../Ascentis.SignalR.Kafka.IntegrationTests.Server/bin/Release/net6.0/", "Ascentis.SignalR.Kafka.IntegrationTests.Server.exe" });
        Console.WriteLine(serverPath);

        foreach (var port in ports)
        {
            var processStartInfo = new ProcessStartInfo
            {
                WorkingDirectory = workingDirectory,
                FileName = serverPath,
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