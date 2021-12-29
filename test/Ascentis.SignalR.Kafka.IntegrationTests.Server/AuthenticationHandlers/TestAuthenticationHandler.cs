using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ascentis.SignalR.Kafka.IntegrationTests.Server.AuthenticationHandlers;

public class TestAuthenticationHandler
    : AuthenticationHandler<AuthenticationSchemeOptions>
{
    public static readonly string SchemeName = "TestAuthScheme";

    public TestAuthenticationHandler(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder, ISystemClock clock) : base(options, logger, encoder, clock)
    {
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        const string authTokenHeader = "TestAuthHeader";
        const string authenticationFailure = "authentication failure";

        var headerValue = Request.Headers[authTokenHeader].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(headerValue))
            return Task.FromResult(AuthenticateResult.Fail(authenticationFailure));

        try
        {
            var claims = new List<Claim>
            {
                new Claim(ClaimTypes.NameIdentifier, headerValue)
            };
            var claimsIdentity = new ClaimsIdentity(claims, SchemeName);
            var claimsPrincipal = new ClaimsPrincipal(claimsIdentity);
            var ticket = new AuthenticationTicket(claimsPrincipal, Scheme.Name);
            return Task.FromResult(AuthenticateResult.Success(ticket));
        }
        catch (Exception e)
        {
            Logger.Log(LogLevel.Error, e, "Exception building authentication ticket", headerValue);
            return Task.FromResult(AuthenticateResult.Fail(authenticationFailure));
        }
    }
}