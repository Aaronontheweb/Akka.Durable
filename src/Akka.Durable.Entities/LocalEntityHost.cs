using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Event;
using Akka.Reminders;

namespace Akka.Durable.Entities;

/// <summary>
/// Child-per-entity parent that stands in for a shard region in local (non-clustered) execution.
/// </summary>
/// <remarks>
/// Speaks the same protocol as a shard: it accepts <see cref="ShardingEnvelope"/>s, lazily creates one
/// child per entity id, and honours <see cref="Passivate"/> by buffering that entity's messages until it
/// has stopped, then reactivating it if anything arrived in the meantime. Only one child per entity id
/// exists at a time.
/// </remarks>
internal sealed class LocalEntityHost : ReceiveActor
{
    private readonly Func<string, Props> _entityProps;
    private readonly Dictionary<string, IActorRef> _entities = [];
    private readonly Dictionary<IActorRef, string> _entityIds = [];
    private readonly Dictionary<string, List<(object Message, IActorRef Sender)>> _passivating = [];
    private readonly ILoggingAdapter _log = Context.GetLogger();

    /// <summary>Top-level actor name of the host for an entity type.</summary>
    public static string ActorName(EntityTypeName typeName) => ActorName(typeName.Value);

    private static string ActorName(string typeName) => $"durable-{Uri.EscapeDataString(typeName)}";

    /// <summary>The running host for an entity type, if this system hosts it locally.</summary>
    public static IActorRef? TryGet(ActorSystem system, string typeName)
    {
        var host = ((ExtendedActorSystem)system).Guardian.GetChild([ActorName(typeName)]);
        return host.IsNobody() ? null : host;
    }

    public LocalEntityHost(Func<string, Props> entityProps)
    {
        _entityProps = entityProps;

        Receive<ShardingEnvelope>(e => Deliver(e.EntityId, e.Message));

        // Akka.Reminders' test resolver delivers the bare envelope rather than wrapping it.
        Receive<ReminderEnvelope>(e => Deliver(e.Entity.EntityId, e));

        Receive<Passivate>(HandlePassivate);
        Receive<Terminated>(HandleTerminated);
    }

    private void Deliver(string entityId, object message)
    {
        if (_passivating.TryGetValue(entityId, out var buffer))
        {
            buffer.Add((message, Sender));
            return;
        }

        GetOrCreate(entityId).Forward(message);
    }

    private IActorRef GetOrCreate(string entityId)
    {
        if (_entities.TryGetValue(entityId, out var existing))
            return existing;

        var entity = Context.ActorOf(_entityProps(entityId), Uri.EscapeDataString(entityId));
        Context.Watch(entity);
        _entities[entityId] = entity;
        _entityIds[entity] = entityId;
        return entity;
    }

    private void HandlePassivate(Passivate passivate)
    {
        if (!_entityIds.TryGetValue(Sender, out var entityId))
        {
            _log.Warning("Ignoring Passivate from {0}, which is not an entity hosted here", Sender);
            return;
        }

        if (_passivating.ContainsKey(entityId)) return;

        _passivating[entityId] = [];
        Sender.Tell(passivate.StopMessage);
    }

    private void HandleTerminated(Terminated terminated)
    {
        if (!_entityIds.Remove(terminated.ActorRef, out var entityId)) return;
        _entities.Remove(entityId);

        if (!_passivating.Remove(entityId, out var buffer) || buffer.Count == 0) return;

        // Messages arrived while the entity was passivating: start a new incarnation for them.
        var entity = GetOrCreate(entityId);
        foreach (var (message, sender) in buffer)
            entity.Tell(message, sender);
    }
}
