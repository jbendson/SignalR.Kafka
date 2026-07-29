using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Ascentis.SignalR.Kafka.Extensions
{
    internal static class TaskExtensions
    {
        internal static async void FireAndForget(this Task task, ILogger logger)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "FireAndForget exception");
            }
        }
    }
}
