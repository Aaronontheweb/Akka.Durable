using Akka.Actor;
using Akka.Serialization.V2;

namespace Akka.Durable;

/// <summary>
/// Logical reference to one entity: its entity type and <see cref="EntityId"/>.
/// </summary>
/// <typeparam name="TCommand">The commands supported by this actor.</typeparam>
/// <remarks>
/// <para>Implements <see cref="ICanTell"/> for interop and compatibility purposes with classic actors.</para>
/// <para>A ref is bound to this node's shard region or local host and is not serializable. To refer to an
/// entity inside a message, journal event or reminder, carry its <see cref="EntityId"/> (which is
/// serializable) and resolve it on the receiving side with <see cref="IEntityRefs{TCommand}.Get"/>.
/// Serializable typed ref fields are tracked upstream in akkadotnet/akka.net#8911.</para>
/// </remarks>
public interface IEntityRef<in TCommand> : ICanTell
{
    public EntityId Id { get; }

    void Tell(TCommand command, IActorRef? sender = null);

    Task<TReply> Ask<TReply>(
        TCommand command,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Factory for logical entity references of a single entity type, keyed by its command type.
/// </summary>
/// <remarks>
/// Creating a reference is cheap, synchronous and side-effect free: it does not activate the entity.
/// The entity is activated by the first message sent through the reference.
/// <para>
/// Refs are keyed by command type, so each entity type must have its own command type (typically an
/// entity-specific command union). Registering two entity types with the same command type makes the
/// second registration collide in the ActorRegistry.
/// </para>
/// </remarks>
/// <typeparam name="TCommand">The command protocol of the entity type.</typeparam>
public interface IEntityRefs<TCommand>
{
    IEntityRef<TCommand> Get(EntityId id);
}

/// <summary>
/// Entity reference that can also read the entity's current state.
/// </summary>
/// <remarks>
/// Messages identify entities by <see cref="EntityId"/>, which doesn't depend on the state type. To read an
/// entity's state from an ID or a plain <see cref="IEntityRef{TCommand}"/>, get a state-aware ref with
/// <see cref="IDurableEntityRefs{TCommand,TState,TEvent}.Get"/>.
/// <para>
/// <typeparamref name="TEvent"/> is part of the type now so that event subscriptions can be added to this
/// interface without changing its signature.
/// </para>
/// </remarks>
/// <typeparam name="TCommand">The command protocol of the entity type.</typeparam>
/// <typeparam name="TState">The state type of the entity type.</typeparam>
/// <typeparam name="TEvent">The event type of the entity type.</typeparam>
public interface IDurableEntityRef<TCommand, TState, TEvent> : IEntityRef<TCommand>
{
    /// <summary>
    /// Reads the entity's state and the sequence number of the last event applied to it,
    /// activating the entity if needed.
    /// </summary>
    /// <remarks>
    /// The read is queued behind commands the entity has already received: it is answered only after
    /// their events are committed and their effects have finished.
    /// </remarks>
    Task<StateSnapshot<TState>> GetStateAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Factory for state-aware entity references of a single entity type.
/// </summary>
/// <remarks>
/// <typeparamref name="TState"/> and <typeparamref name="TEvent"/> must be the state and event types the
/// entity type was registered with; otherwise <see cref="Get"/> fails because no matching entity type is
/// registered.
/// </remarks>
/// <typeparam name="TCommand">The command protocol of the entity type.</typeparam>
/// <typeparam name="TState">The state type of the entity type.</typeparam>
/// <typeparam name="TEvent">The event type of the entity type.</typeparam>
public interface IDurableEntityRefs<TCommand, TState, TEvent>
{
    IDurableEntityRef<TCommand, TState, TEvent> Get(EntityId id);
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