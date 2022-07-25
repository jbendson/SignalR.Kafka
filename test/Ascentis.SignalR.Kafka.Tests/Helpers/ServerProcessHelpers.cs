using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Ascentis.SignalR.Kafka.IntegrationTests.Extensions;

public static class ServerProcessHelpers
{
    public static List<Process> InitServers(int[] ports)
    {
        var servers = new List<Process>();
        var currentDirectory = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
        var workingDirectory = Path.Join(new string[] { currentDirectory, "../../../../Ascentis.SignalR.Kafka.IntegrationTests.Server/bin/Release/net6.0/" });
        var serverPath = Path.Join(new string[] { currentDirectory, "../../../../Ascentis.SignalR.Kafka.IntegrationTests.Server/bin/Release/net6.0/", "Ascentis.SignalR.Kafka.IntegrationTests.Server.exe" });
        Console.WriteLine(serverPath);
        //SetExecPrivilege("/home/runner/work/SignalR.Kafka/SignalR.Kafka/test/Ascentis.SignalR.Kafka.IntegrationTests.Server/bin/Release/net6.0/Ascentis.SignalR.Kafka.IntegrationTests.Server.dll");

        foreach (var port in ports)
        {
            var processStartInfo = new ProcessStartInfo
            {
                WorkingDirectory = "/home/runner/work/SignalR.Kafka/SignalR.Kafka/test/Ascentis.SignalR.Kafka.IntegrationTests.Server/bin/Release/net6.0/",
                FileName = "Ascentis.SignalR.Kafka.IntegrationTests.Server.dll",
                UseShellExecute = true,
                CreateNoWindow = false,
                Arguments = port.ToString(),
                //RedirectStandardOutput = true,
                //RedirectStandardError = true
            };

            var server = Process.Start(processStartInfo);
            /*
            server.OutputDataReceived += new DataReceivedEventHandler((sender, e) => Console.WriteLine($"[server-{port}-output] {e.Data}"));
            server.BeginOutputReadLine();
            server.ErrorDataReceived += new DataReceivedEventHandler((sender, e) => Console.WriteLine($"[server-{port}-error] {e.Data}"));
            server.BeginErrorReadLine();
            */
            servers.Add(server);
        }

        return servers;
    }

    private static void SetExecPrivilege(string cmd)
    {
        var escapedArgs = cmd.Replace("\"", "\\\"");

        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden,
                FileName = "/bin/bash",
                Arguments = $"-c \"{escapedArgs}\""
            }
        };

        process.Start();
        process.WaitForExit();
    }
}