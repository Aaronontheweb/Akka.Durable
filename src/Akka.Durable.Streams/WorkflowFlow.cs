using Akka.Streams.Dsl;

namespace Akka.Durable.Streams;

/// <summary>
/// Flows that start durable workflows, one per stream element.
/// </summary>
/// <remarks>
/// <para>
/// A workflow is durable and may run for a long time: it can wait on a timer for months. So starting one and
/// waiting for it are separate stages here. <see cref="Start"/> is bounded and finishes as soon as every start is
/// stored. <see cref="Wait"/> observes outcomes and never holds up a start. <see cref="Run"/> does both, and is for
/// workflows that finish in seconds.
/// </para>
/// <para>
/// The stream is not durable. If the process stops, the workflows resume and the stream does not. The source must
/// replay from an offset of its own, for example by committing only after an element's outcome is emitted. Compute
/// the workflow id from the element: a replayed element then reaches the instance it started before and starts no
/// second one.
/// </para>
/// </remarks>
public static class WorkflowFlow
{
    /// <summary>
    /// Starts one workflow per element and emits the id with the start's answer. An element is emitted once its
    /// start is stored, so a slow journal slows the source down. Output keeps the input's order.
    /// </summary>
    /// <remarks>
    /// This is the stage to use for a workflow that may run for a long time: it never waits for an outcome, so the
    /// stream completes as soon as every start is stored. Observe the outcomes with <see cref="Wait"/>.
    /// </remarks>
    public static Flow<TInput, (WorkflowId Id, StartResult<TOutcome> Result), NotUsed> Start<
        TInput, TState, TFact, TOutcome>(
        IWorkflowRefs<TInput, TState, TFact, TOutcome> workflows,
        Func<TInput, WorkflowId> idOf,
        int parallelism)
        where TState : notnull
    {
        ArgumentNullException.ThrowIfNull(workflows);
        ArgumentNullException.ThrowIfNull(idOf);
        ArgumentOutOfRangeException.ThrowIfLessThan(parallelism, 1);

        return Flow.Create<TInput>()
            .SelectAsync(parallelism, async input =>
            {
                var id = idOf(input);
                return (id, await workflows.Get(id).Start(input));
            });
    }

    /// <summary>
    /// Waits for the outcome of each workflow named by an element, and emits it. Output keeps the input's order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each element waits in its own substream, so a workflow that takes a long time does not stop the others from
    /// being waited on. At most <paramref name="breadth"/> waits are open at once.
    /// </para>
    /// <para>
    /// An element whose workflow does not end within <paramref name="timeout"/> emits
    /// <see cref="WorkflowTimedOut"/>. The workflow itself carries on: it is durable, and its outcome is still in
    /// its journal.
    /// </para>
    /// </remarks>
    public static Flow<(WorkflowId Id, TInput Input), (WorkflowId Id, WorkflowEnded<TOutcome> Ended), NotUsed> Wait<
        TInput, TState, TFact, TOutcome>(
        IWorkflowRefs<TInput, TState, TFact, TOutcome> workflows,
        int breadth,
        TimeSpan timeout)
        where TState : notnull
    {
        ArgumentNullException.ThrowIfNull(workflows);
        ArgumentOutOfRangeException.ThrowIfLessThan(breadth, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

        return Flow.Create<(WorkflowId Id, TInput Input)>()
            .MergeMany(breadth, element => Source.FromTask(
                Await(workflows.Get(element.Id), element.Id, timeout)));
    }

    /// <summary>
    /// Starts one workflow per element and emits its outcome, in the input's order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is <see cref="Start"/> followed by <see cref="Wait"/>, for a workflow that finishes in seconds. A
    /// workflow that waits on a timer for months would hold an element open for months, so use
    /// <see cref="Start"/> and observe the outcomes separately for those.
    /// </para>
    /// <para>
    /// An element whose workflow does not end within <paramref name="timeout"/> emits
    /// <see cref="WorkflowTimedOut"/>. The workflow itself carries on.
    /// </para>
    /// </remarks>
    public static Flow<TInput, (WorkflowId Id, WorkflowEnded<TOutcome> Ended), NotUsed> Run<
        TInput, TState, TFact, TOutcome>(
        IWorkflowRefs<TInput, TState, TFact, TOutcome> workflows,
        Func<TInput, WorkflowId> idOf,
        int parallelism,
        TimeSpan timeout)
        where TState : notnull
    {
        ArgumentNullException.ThrowIfNull(workflows);
        ArgumentNullException.ThrowIfNull(idOf);
        ArgumentOutOfRangeException.ThrowIfLessThan(parallelism, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

        return Flow.Create<TInput>()
            .SelectAsync(parallelism, async input =>
            {
                var id = idOf(input);
                var workflow = workflows.Get(id);

                // Idempotent: a replayed element reaches the workflow it started before. A workflow that has
                // already finished is read rather than started, so the outcome carries when it finished.
                await workflow.Start(input);

                return await Await(workflow, id, timeout);
            });
    }

    /// <summary>
    /// Waits for one workflow's outcome, and turns a wait that ran out into <see cref="WorkflowTimedOut"/>.
    /// </summary>
    private static async Task<(WorkflowId Id, WorkflowEnded<TOutcome> Ended)> Await<
        TInput, TState, TFact, TOutcome>(
        IWorkflowRef<TInput, TState, TFact, TOutcome> workflow, WorkflowId id, TimeSpan timeout)
        where TState : notnull
    {
        using var limit = new CancellationTokenSource(timeout);
        try
        {
            // The snapshot, not WaitForOutcome: it carries when the workflow finished.
            var snapshot = await workflow.WaitForStateAsync(
                status => status.Value is WorkflowFinished<TOutcome> or WorkflowNotStarted, limit.Token);

            return (id, snapshot.State.Value switch
            {
                WorkflowFinished<TOutcome> finished => finished,
                _ => new WorkflowNotStarted(),
            });
        }
        catch (OperationCanceledException) when (limit.IsCancellationRequested)
        {
            return (id, new WorkflowTimedOut());
        }
    }
}
