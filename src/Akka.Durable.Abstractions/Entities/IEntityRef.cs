using Akka.Actor;
using Akka.Serialization.V2;
using Akka.Streams;

namespace Akka.Durable.Abstractions.Entities;

/// <summary>
/// Strongly typed representation of an entity actor.
/// </summary>
/// <typeparam name="TCommand">The commands supported by this actor.</typeparam>
/// <remarks>
/// Implements <see cref="ICanTell"/> for interop and compatibility purposes with classic actors.
/// </remarks>
public interface IEntityRef<in TCommand> : ICanTell
{
    public EntityId Id { get; }

    void Tell(TCommand command, IActorRef? sender = null);

    Task<TReply> Ask<TReply>(
        TCommand command,
        CancellationToken cancellationToken = default);
}

public interface IDurableEntityRef<TCommand, TState, TEvent>
    : IEntityRef<TCommand>
{
    Task<StateSnapshot<TState>> GetStateAsync();

    Task<ISourceRef<EntityUpdate<TState, TEvent>>>
        SubscribeAsync();
}

[AkkaSerializable]
public sealed record StateSnapshot<TState>(
    [property: AkkaField(0)] TState State,
    [property: AkkaField(1)] long SequenceNr);

[AkkaSerializable]
public sealed record CommittedEvent<TEvent>(
    [property: AkkaField(0)] TEvent Event,
    [property: AkkaField(1)] long SequenceNr);

public readonly union EntityUpdate<TState, TEvent>(
    StateSnapshot<TState>,
    CommittedEvent<TEvent>);