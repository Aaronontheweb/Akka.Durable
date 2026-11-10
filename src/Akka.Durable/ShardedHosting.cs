using Akka.Cluster.Sharding;

namespace Akka.Durable;

/// <summary>
/// Hosts a durable entity type or workflow type in an Akka.Cluster.Sharding region. Pass it to
/// <c>WithDurableEntity</c> or <c>WithDurableWorkflow</c>. Without it, the type is hosted in a local host
/// actor in this process.
/// </summary>
/// <remarks>
/// These are the sharding settings an application chooses. Akka.Durable sets the others itself:
/// the message that stops an entity, and idle passivation, which comes from
/// <see cref="EntityOptions.PassivateIdleAfter"/>. The number of shards is
/// <see cref="EntityOptions.NumberOfShards"/>, on the definition, because every node must agree on it.
/// </remarks>
public sealed record ShardedHosting
{
    /// <summary>The cluster role whose nodes host the entities. <c>null</c> hosts them on every node.</summary>
    public string? Role { get; init; }

    /// <summary>Where sharding keeps its own state. <c>null</c> uses the actor system's configured default.</summary>
    public StateStoreMode? StateStoreMode { get; init; }

    /// <summary>
    /// Start every entity again after its shard moves or its node restarts. <c>null</c> uses the actor
    /// system's configured default.
    /// </summary>
    /// <remarks>
    /// Sharding does not passivate idle entities while this is on, so
    /// <see cref="EntityOptions.PassivateIdleAfter"/> has no effect in sharded mode.
    /// </remarks>
    public bool? RememberEntities { get; init; }

    /// <summary>Where sharding remembers its entities. <c>null</c> uses the actor system's configured default.</summary>
    public RememberEntitiesStore? RememberEntitiesStore { get; init; }
}
