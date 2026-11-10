using Akka.Actor;

namespace Akka.Durable;

/// <summary>
/// The message a host sends a durable entity to stop it: after <c>Passivate</c>, on idle passivation, and as the
/// shard's hand-off stop message. One type and one instance serve every entity type, so a <c>ShardOptions</c>
/// object reused for several entity types stops all of them.
/// </summary>
/// <remarks>
/// <para>An entity with an <c>Execute</c> handler in flight stops only once that handler's outcome is decided.</para>
/// <para>Only ever sent by an entity's own parent (its shard or local host) on the same node, so it never crosses
/// a serialization boundary.</para>
/// </remarks>
internal sealed class StopEntity : INoSerializationVerificationNeeded
{
    public static readonly StopEntity Instance = new();
    private StopEntity() { }
}
