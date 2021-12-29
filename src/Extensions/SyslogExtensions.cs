using System;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Ascentis.SignalR.Kafka.Extensions;

public static class SyslogExtensions
{
    public static LogLevel ToLogLevel(this SyslogLevel sysLogLevel)
    {
        switch (sysLogLevel)
        {
            case SyslogLevel.Emergency:
                return LogLevel.Critical;
            case SyslogLevel.Alert:
                return LogLevel.Critical;
            case SyslogLevel.Critical:
                return LogLevel.Critical;
            case SyslogLevel.Error:
                return LogLevel.Error;
            case SyslogLevel.Warning:
                return LogLevel.Warning;
            case SyslogLevel.Notice:
                return LogLevel.Information;
            case SyslogLevel.Info:
                return LogLevel.Information;
            case SyslogLevel.Debug:
                return LogLevel.Debug;
            default:
                throw new ArgumentOutOfRangeException(nameof(sysLogLevel), sysLogLevel, null);
        }
    }
}