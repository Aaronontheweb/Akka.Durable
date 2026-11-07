using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Abstractions.Entities;

namespace Akka.Durable.Entities;

/// <summary>
/// Logical reference to one entity. Messages go through the entity type's shard region or local host,
/// which create the entity on demand and buffer messages while it passivates.
/// </summary>
internal sealed class EntityRef<TCommand>(EntityId id, IActorRef region) : IEntityRef<TCommand>
{
    public EntityId Id => id;

    public void Tell(TCommand command, IActorRef? sender = null) =>
        region.Tell(new ShardingEnvelope(id.Value, command!), sender!);

    public Task<TReply> Ask<TReply>(TCommand command, CancellationToken cancellationToken = default) =>
        region.Ask<TReply>(new ShardingEnvelope(id.Value, command!), cancellationToken);

    void ICanTell.Tell(object message, IActorRef sender)
    {
        if (message is not TCommand command)
            throw new ArgumentException(
                $"Entity {id.Value} only accepts {typeof(TCommand).FullName}, but received {message?.GetType().FullName ?? "null"}. " +
                $"If {typeof(TCommand).Name} is a union, convert the case value to it before sending.",
                nameof(message));

        Tell(command, sender);
    }
}

/// <summary>
/// <see cref="EntityRef{TCommand}"/> that can also read the entity's state.
/// </summary>
internal sealed class EntityRef<TCommand, TState>(EntityId id, IActorRef region) : IDurableEntityRef<TCommand, TState>
{
    private readonly EntityRef<TCommand> _ref = new(id, region);

    public EntityId Id => id;

    public void Tell(TCommand command, IActorRef? sender = null) => _ref.Tell(command, sender);

    public Task<TReply> Ask<TReply>(TCommand command, CancellationToken cancellationToken = default) =>
        _ref.Ask<TReply>(command, cancellationToken);

    void ICanTell.Tell(object message, IActorRef sender) => ((ICanTell)_ref).Tell(message, sender);

    public Task<StateSnapshot<TState>> GetStateAsync(CancellationToken cancellationToken = default) =>
        region.Ask<StateSnapshot<TState>>(new ShardingEnvelope(id.Value, GetEntityState.Instance), cancellationToken);
}
