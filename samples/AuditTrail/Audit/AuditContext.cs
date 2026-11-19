namespace Akka.Durable.Samples.AuditTrail.Audit;

/// <summary>Who is acting, through which door, and the id of this one action. It lives in an AsyncLocal.</summary>
public sealed record AuditContext(string Who, string Door, string ActionId)
{
    private static readonly AsyncLocal<AuditContext?> Ambient = new();

    public static AuditContext? Current
    {
        get => Ambient.Value;
        set => Ambient.Value = value;
    }

    /// <summary>A new action. The server makes the action id, never the caller.</summary>
    public static AuditContext Start(string who, string door) => new(who, door, Guid.NewGuid().ToString("N")[..8]);
}
