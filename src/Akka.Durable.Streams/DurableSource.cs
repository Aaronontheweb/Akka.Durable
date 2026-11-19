using Akka.Streams.Dsl;

namespace Akka.Durable.Streams;

/// <summary>
/// Sources over the state of one entity or one workflow.
/// </summary>
public static class DurableSource
{
    /// <summary>
    /// The entity's current snapshot, then one after each later write that holds events.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This source never completes by itself: an entity has no last state. Bound it, for example with
    /// <c>TakeWhile</c>, <c>TakeWithin</c> or <c>IdleTimeout</c>. When the stream stops, the subscription ends.
    /// </para>
    /// <para>
    /// This is the entity's <see cref="IDurableEntityRef{TCommand,TState,TEvent}.WatchStateAsync"/> as a source. It
    /// is a live push held in memory, not durable delivery. A slow reader gets the newest snapshot it has not read:
    /// it can skip states, but it never reads an older state after a newer one. Watching activates the entity, so a
    /// source that stays materialized keeps it alive.
    /// </para>
    /// </remarks>
    public static Source<StateSnapshot<TState>, NotUsed> Watch<TCommand, TState, TEvent>(
        IDurableEntityRef<TCommand, TState, TEvent> entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        return Source.From(() => entity.WatchStateAsync());
    }

    /// <summary>
    /// The workflow's current snapshot, then one after each later write that holds events. Completes after the
    /// <see cref="WorkflowFinished{TOutcome}"/> snapshot, because a finished workflow does not change.
    /// </summary>
    /// <remarks>
    /// A workflow that was never started does not complete this source. Bound it, as for an entity. When the stream
    /// stops, the subscription ends. A slow reader gets the newest snapshot it has not read, and always reads the
    /// finished one. Watching activates the workflow, so a source that stays materialized keeps it alive.
    /// </remarks>
    public static Source<StateSnapshot<WorkflowStatus<TState, TOutcome>>, NotUsed> Watch<
        TInput, TState, TFact, TOutcome>(
        IWorkflowRef<TInput, TState, TFact, TOutcome> workflow)
        where TState : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);
        return Source.From(() => workflow.WatchStateAsync());
    }
}
