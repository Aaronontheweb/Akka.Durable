namespace Akka.Durable.Samples.AuditTrail.Audit;

/// <summary>Carries the audit context beside each command, and puts it back before an effect handler runs.</summary>
public sealed class AuditPropagator : IHeaderPropagator
{
    private const string Who = "audit-who", Door = "audit-door", Action = "audit-action";

    public void Inject(IDictionary<string, string> headers)
    {
        if (AuditContext.Current is { } context) Write(context, headers);
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> headers)
    {
        var before = AuditContext.Current;
        AuditContext.Current = Read(headers);
        return new Undo(() => AuditContext.Current = before);
    }

    // A timer is a starting point of its own: nobody asked. The system acts, and the door names the timer.
    public void InjectForTimer(EntityTypeName entityType, EntityId entityId, string key, IDictionary<string, string> headers) =>
        Write(AuditContext.Start("system", $"timer {key}"), headers);

    /// <summary>The audit context that the headers of an event carry, or null.</summary>
    public static AuditContext? Read(IReadOnlyDictionary<string, string> headers) =>
        headers.TryGetValue(Who, out var who)
            ? new AuditContext(who, headers.GetValueOrDefault(Door, "?"), headers.GetValueOrDefault(Action, "?"))
            : null;

    private static void Write(AuditContext context, IDictionary<string, string> headers)
    {
        headers[Who] = context.Who;
        headers[Door] = context.Door;
        headers[Action] = context.ActionId;
    }

    private sealed class Undo(Action undo) : IDisposable
    {
        public void Dispose() => undo();
    }
}
