using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Event;
using Akka.Reminders;

namespace Akka.Durable;

/// <summary>
/// Child-per-entity parent that stands in for a shard region in local (non-clustered) execution.
/// </summary>
/// <remarks>
/// <para>Speaks the same protocol as a shard: it accepts <see cref="ShardingEnvelope"/>s, lazily creates one
/// child per entity id, and honours <see cref="Passivate"/> by buffering that entity's messages until it
/// has stopped, then reactivating it if anything arrived in the meantime. Only one child per entity id
/// exists at a time.</para>
/// <para>Like a shard, it also passivates entities that have received no message for the idle timeout,
/// using the same buffered protocol with the stop message it was constructed with.</para>
/// </remarks>
internal sealed class LocalEntityHost : ReceiveActor, IWithTimers
{
    private readonly Func<string, Props> _entityProps;
    private readonly object _stopMessage;
    private readonly long _passivateIdleAfterMs;
    private readonly Dictionary<string, IActorRef> _entities = [];
    private readonly Dictionary<IActorRef, string> _entityIds = [];
    private readonly Dictionary<string, long> _lastActivity = [];
    private readonly Dictionary<string, List<(object Message, IActorRef Sender)>> _passivating = [];
    private readonly ILoggingAdapter _log = Context.GetLogger();

    public ITimerScheduler Timers { get; set; } = null!;

    /// <summary>Top-level actor name of the host for an entity type.</summary>
    public static string ActorName(EntityTypeName typeName) => ActorName(typeName.Value);

    private static string ActorName(string typeName) => $"durable-{Uri.EscapeDataString(typeName)}";

    /// <summary>The running host for an entity type, if this system hosts it locally.</summary>
    public static IActorRef? TryGet(ActorSystem system, string typeName)
    {
        var host = ((ExtendedActorSystem)system).Guardian.GetChild([ActorName(typeName)]);
        return host.IsNobody() ? null : host;
    }

    /// <param name="entityProps">Props for the entity child with the given id.</param>
    /// <param name="stopMessage">Sent to an entity to stop it when the host passivates it for being idle.</param>
    /// <param name="passivateIdleAfter">Idle timeout; non-positive disables idle passivation.</param>
    public LocalEntityHost(Func<string, Props> entityProps, object stopMessage, TimeSpan passivateIdleAfter)
    {
        _entityProps = entityProps;
        _stopMessage = stopMessage;
        _passivateIdleAfterMs = (long)passivateIdleAfter.TotalMilliseconds;

        Receive<ShardingEnvelope>(e => Deliver(e.EntityId, e.Message));

        // Akka.Reminders' test resolver delivers the bare envelope rather than wrapping it.
        Receive<ReminderEnvelope>(e => Deliver(e.Entity.EntityId, e));

        Receive<Passivate>(HandlePassivate);
        Receive<Terminated>(HandleTerminated);
        Receive<PassivateIdleTick>(_ => PassivateIdleEntities());
    }

    protected override void PreStart()
    {
        if (_passivateIdleAfterMs <= 0) return;

        // Same cadence as a shard: check at half the timeout, so an entity is stopped within 1.5x of it.
        var interval = TimeSpan.FromMilliseconds(Math.Max(1, _passivateIdleAfterMs / 2));
        Timers.StartPeriodicTimer(PassivateIdleTick.Instance, PassivateIdleTick.Instance, interval);
    }

    private void Deliver(string entityId, object message)
    {
        if (_passivating.TryGetValue(entityId, out var buffer))
        {
            buffer.Add((message, Sender));
            return;
        }

        var entity = GetOrCreate(entityId);
        _lastActivity[entityId] = Environment.TickCount64;
        entity.Forward(message);
    }

    private IActorRef GetOrCreate(string entityId)
    {
        if (_entities.TryGetValue(entityId, out var existing))
            return existing;

        var entity = Context.ActorOf(_entityProps(entityId), Uri.EscapeDataString(entityId));
        Context.Watch(entity);
        _entities[entityId] = entity;
        _entityIds[entity] = entityId;
        _lastActivity[entityId] = Environment.TickCount64;
        return entity;
    }

    private void HandlePassivate(Passivate passivate)
    {
        if (!_entityIds.TryGetValue(Sender, out var entityId))
        {
            _log.Warning("Ignoring Passivate from {0}, which is not an entity hosted here", Sender);
            return;
        }

        StartPassivation(entityId, Sender, passivate.StopMessage);
    }

    private void PassivateIdleEntities()
    {
        var deadline = Environment.TickCount64 - _passivateIdleAfterMs;
        List<string>? idle = null;
        foreach (var (entityId, lastActivity) in _lastActivity)
        {
            if (lastActivity <= deadline && !_passivating.ContainsKey(entityId))
                (idle ??= []).Add(entityId);
        }

        if (idle is null) return;

        _log.Debug("Passivating {0} idle entities", idle.Count);
        foreach (var entityId in idle)
            StartPassivation(entityId, _entities[entityId], _stopMessage);
    }

    /// <summary>Buffers the entity's messages from now on and asks it to stop.</summary>
    private void StartPassivation(string entityId, IActorRef entity, object stopMessage)
    {
        if (_passivating.ContainsKey(entityId)) return;

        _passivating[entityId] = [];
        entity.Tell(stopMessage);
    }

    private void HandleTerminated(Terminated terminated)
    {
        if (!_entityIds.Remove(terminated.ActorRef, out var entityId)) return;
        _entities.Remove(entityId);
        _lastActivity.Remove(entityId);

        if (!_passivating.Remove(entityId, out var buffer) || buffer.Count == 0) return;

        // Messages arrived while the entity was passivating: start a new incarnation for them.
        var entity = GetOrCreate(entityId);
        foreach (var (message, sender) in buffer)
            entity.Tell(message, sender);
    }

    // Timer message the host sends itself; never leaves this actor.
    private sealed class PassivateIdleTick : INoSerializationVerificationNeeded
    {
        public static readonly PassivateIdleTick Instance = new();
        private PassivateIdleTick() { }
    }
}
