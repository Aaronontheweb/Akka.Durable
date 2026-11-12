using Akka.Actor;
using Akka.Serialization.V2;

namespace Akka.Durable;

/// <summary>
/// Logical reference to one entity: its entity type and <see cref="EntityId"/>.
/// </summary>
/// <typeparam name="TCommand">The commands supported by this actor.</typeparam>
/// <remarks>
/// <para>Deliberately does not implement <see cref="ICanTell"/>: its untyped Ask extensions would bypass
/// the command's reply contract.</para>
/// <para>A ref is bound to this node's shard region or local host and is not serializable. To refer to an
/// entity inside a message, journal event or reminder, carry its <see cref="EntityId"/> (which is
/// serializable) and resolve it on the receiving side with <see cref="IEntityRefs{TCommand}.Get"/>.
/// Serializable typed ref fields are tracked upstream in akkadotnet/akka.net#8911.</para>
/// </remarks>
public interface IEntityRef<in TCommand>
{
    public EntityId Id { get; }

    void Tell(TCommand command, IActorRef? sender = null);

    Task<TReply> Ask<TReply>(
        IReplies<TCommand, TReply> command,
        CancellationToken cancellationToken = default) where TReply : notnull;
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
public interface IEntityRefs<in TCommand>
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
/// No member uses <typeparamref name="TEvent"/> yet. It is part of the type so that event subscriptions can be added
/// later without changing the signature.
/// </para>
/// </remarks>
/// <typeparam name="TCommand">The command protocol of the entity type.</typeparam>
/// <typeparam name="TState">The state type of the entity type.</typeparam>
/// <typeparam name="TEvent">The event type of the entity type.</typeparam>
public interface IDurableEntityRef<in TCommand, TState, TEvent> : IEntityRef<TCommand>
{
    /// <summary>
    /// Reads the entity's state and the sequence number of the last event applied to it,
    /// activating the entity if needed.
    /// </summary>
    /// <remarks>
    /// The read is queued behind commands the entity has already received: it is answered only after
    /// their events are committed and their Run effects have finished. Execute effects are not awaited: the
    /// read may return while one is in flight.
    /// </remarks>
    Task<StateSnapshot<TState>> GetStateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits until the entity's state satisfies <paramref name="until"/>, and returns that snapshot. Returns after
    /// one round trip when the current state already satisfies it. Activates the entity if needed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The predicate runs in this process, once for each snapshot that reaches the wait, in order: the current
    /// state first, then the state after each later write that holds events. It is never sent to the entity. Keep it
    /// short and do not block in it.
    /// </para>
    /// <para>
    /// Each snapshot is one that <see cref="GetStateAsync"/> could return: it is sent after the write's events are
    /// committed and its Run effects have finished. This is a live push held in memory, not durable delivery. When
    /// the entity stops, passivates, restarts or moves, the wait subscribes again by itself and goes on from the state
    /// the entity has then, so a state that held only in between is not seen.
    /// </para>
    /// </remarks>
    /// <param name="until">The condition on the state.</param>
    /// <param name="cancellationToken">The time limit of the wait. Required.</param>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> fired first.</exception>
    Task<StateSnapshot<TState>> WaitForStateAsync(Func<TState, bool> until, CancellationToken cancellationToken);

    /// <summary>
    /// The entity's current snapshot, then a snapshot after each later write that holds events. Activates the
    /// entity if needed. The subscription starts when the enumeration starts and ends when the reader stops.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A slow reader gets the newest snapshot it has not read: it can skip states, but it never reads an older
    /// state after a newer one. <see cref="StateSnapshot{TState}.SequenceNr"/> grows from one snapshot to the next.
    /// </para>
    /// <para>
    /// This is a live push held in memory, not durable delivery. When the entity stops, passivates, restarts or
    /// moves, the stream subscribes again by itself and goes on from the state the entity has then. A reaction that
    /// must happen belongs in an effect.
    /// </para>
    /// </remarks>
    IAsyncEnumerable<StateSnapshot<TState>> WatchStateAsync(CancellationToken cancellationToken = default);
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
public interface IDurableEntityRefs<in TCommand, TState, TEvent>
{
    IDurableEntityRef<TCommand, TState, TEvent> Get(EntityId id);
}

[AkkaSerializable]
public sealed record StateSnapshot<TState>(
    [property: AkkaField(0)] TState State,
    [property: AkkaField(1)] long SequenceNr);
