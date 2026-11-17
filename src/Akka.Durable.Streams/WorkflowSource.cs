using Akka.Streams.Dsl;

namespace Akka.Durable.Streams;

/// <summary>
/// Sources over a durable workflow's condition.
/// </summary>
public static class WorkflowSource
{
    /// <summary>
    /// The workflow's current condition, then one after each later write that holds events. Completes after the
    /// <see cref="WorkflowFinished{TOutcome}"/> snapshot, because a finished workflow does not change.
    /// </summary>
    /// <remarks>
    /// This is the workflow's <see cref="IWorkflowRef{TInput,TState,TFact,TOutcome}.WatchStateAsync"/> as a source.
    /// A slow reader gets the newest snapshot it has not read, and always reads the finished one. Watching
    /// activates the workflow, so a source that stays materialized keeps it alive.
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
