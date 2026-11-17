using Akka.Streams.Dsl;

namespace Akka.Durable.Streams;

/// <summary>
/// Sources over a durable entity's state.
/// </summary>
public static class EntitySource
{
    /// <summary>
    /// The entity's current snapshot, then one after each later write that holds events. Never completes: an
    /// entity has no terminal state.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the entity's <see cref="IDurableEntityRef{TCommand,TState,TEvent}.WatchStateAsync"/> as a source. It
    /// is a live push held in memory, not durable delivery: when the entity stops, passivates, restarts or moves,
    /// the subscription starts again by itself and goes on from the state the entity has then.
    /// </para>
    /// <para>
    /// A slow reader gets the newest snapshot it has not read. It can skip states, but it never reads an older
    /// state after a newer one. Watching activates the entity, so a source that stays materialized keeps it alive.
    /// </para>
    /// </remarks>
    public static Source<StateSnapshot<TState>, NotUsed> Watch<TCommand, TState, TEvent>(
        IDurableEntityRef<TCommand, TState, TEvent> entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        return Source.From(() => entity.WatchStateAsync());
    }
}
