using Akka.Actor;
using Akka.Cluster.Sharding;

namespace Akka.Durable;

/// <summary>
/// Logical reference to one entity. Messages go through the entity type's shard region or local host,
/// which create the entity on demand and buffer messages while it passivates.
/// </summary>
internal sealed class EntityRef<TCommand>(EntityId id, IActorRef region) : IEntityRef<TCommand>
{
    public EntityId Id => id;

    public void Tell(TCommand command, IActorRef? sender = null) =>
        region.Tell(new ShardingEnvelope(id.Value, command!), sender!);

    public Task<TReply> Ask<TReply>(IReplies<TCommand, TReply> command, CancellationToken cancellationToken = default)
        where TReply : notnull
    {
        ArgumentNullException.ThrowIfNull(command);
        return region.Ask<TReply>(new ShardingEnvelope(id.Value, command.ToCommand()!), cancellationToken);
    }
}

/// <summary>
/// <see cref="EntityRef{TCommand}"/> that can also read the entity's state.
/// </summary>
/// <param name="id">The entity.</param>
/// <param name="region">The entity type's shard region or local host.</param>
/// <param name="system">The actor system this ref is used in. State subscriptions run a watcher actor in it.</param>
internal sealed class EntityRef<TCommand, TState, TEvent>(EntityId id, IActorRef region, ActorSystem system)
    : IDurableEntityRef<TCommand, TState, TEvent>
{
    private readonly EntityRef<TCommand> _ref = new(id, region);

    public EntityId Id => id;

    public void Tell(TCommand command, IActorRef? sender = null) => _ref.Tell(command, sender);

    public Task<TReply> Ask<TReply>(IReplies<TCommand, TReply> command, CancellationToken cancellationToken = default)
        where TReply : notnull =>
        _ref.Ask<TReply>(command, cancellationToken);

    public async Task<StateSnapshot<TState>> GetStateAsync(CancellationToken cancellationToken = default)
    {
        var reply = await region.Ask<EntityStateReply>(
            new ShardingEnvelope(id.Value, GetEntityState.Instance), cancellationToken);
        return Read(reply);
    }

    public Task<StateSnapshot<TState>> WaitForStateAsync(Func<TState, bool> until, CancellationToken cancellationToken) =>
        StateSubscription.WaitAsync(system, region, id, Read, until, cancellationToken);

    public IAsyncEnumerable<StateSnapshot<TState>> WatchStateAsync(CancellationToken cancellationToken = default) =>
        StateSubscription.WatchAsync<TState>(system, region, id, Read, last: null, cancellationToken);

    private static StateSnapshot<TState> Read(EntityStateReply reply) => new((TState)reply.State, reply.SequenceNr);
}
