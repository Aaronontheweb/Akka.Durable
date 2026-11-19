using Akka.Actor;
using Akka.Hosting;

namespace Akka.Durable.Samples.AuditTrail.Audit;

/// <summary>Everything the audit trail needs, in three calls.</summary>
public static class AuditSetup
{
    /// <summary>The propagator, the store, and the event stream publisher.</summary>
    public static IServiceCollection AddAuditTrail(this IServiceCollection services) => services
        .AddSingleton<AuditStore>()
        .AddDurableHeaderPropagator<AuditPropagator>()
        .AddDurableEventStreamPublisher();

    /// <summary>Starts the collector.</summary>
    public static AkkaConfigurationBuilder WithAuditCollector(this AkkaConfigurationBuilder builder) =>
        builder.WithActors((system, _, resolver) => system.ActorOf(resolver.Props<AuditCollector>(), "audit-collector"));

    /// <summary>Opens an audit context for each request, and maps <c>GET /audit</c>.</summary>
    public static WebApplication UseAuditTrail(this WebApplication app)
    {
        // A real application takes the person from its sign-in (the authenticated principal), never from a header that
        // a caller can set. The header keeps this sample free of a login.
        app.Use(async (http, next) =>
        {
            var before = AuditContext.Current;
            AuditContext.Current = AuditContext.Start(
                http.Request.Headers["X-User"].FirstOrDefault() ?? "anonymous",
                $"{http.Request.Method} {http.Request.Path}");
            try { await next(); }
            finally { AuditContext.Current = before; }
        });

        app.MapGet("/audit", (AuditStore store) => store.Render());
        return app;
    }
}
