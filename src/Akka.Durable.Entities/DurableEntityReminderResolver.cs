using Akka.Actor;
using Akka.Cluster;
using Akka.Cluster.Sharding;
using Akka.Reminders;
using Akka.Reminders.Sharding;

namespace Akka.Durable.Entities;

/// <summary>
/// Delivers Akka.Reminders to durable entities hosted locally (by their <see cref="LocalEntityHost"/>)
/// or in a shard region. Shard regions alone work with Akka.Reminders' default resolver; this one is
/// needed once any entity type runs in local mode.
/// </summary>
/// <example>
/// builder.WithLocalReminders(r => r.WithResolver(sys => new DurableEntityReminderResolver(sys)));
/// </example>
public sealed class DurableEntityReminderResolver(ActorSystem system) : IShardRegionResolver
{
    private readonly DefaultShardRegionResolver? _sharding =
        ((ExtendedActorSystem)system).Provider is ClusterActorRefProvider ? new DefaultShardRegionResolver(system) : null;

    public IActorRef? TryResolve(ReminderEntity entity) =>
        LocalEntityHost.TryGet(system, entity.ShardRegionName) ?? _sharding?.TryResolve(entity);

    public void DeliverReminder(ReminderEntity entity, ReminderEnvelope envelope, IActorRef? sender = null) =>
        TryResolve(entity)?.Tell(new ShardingEnvelope(entity.EntityId, envelope), sender!);
}
