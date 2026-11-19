using Akka.Streams.Dsl;

namespace Akka.Durable.Streams;

/// <summary>
/// Flows that call entities and workflows, one call per element.
/// </summary>
/// <remarks>
/// Every flow is request and response, and bounded: at most <c>parallelism</c> elements are in flight, and each has
/// <c>timeout</c> to get its answer. A deadline that passes is the answer <see cref="TimedOut"/>, and an exception
/// for one element is <see cref="Failed"/>. The stream carries on. Every output carries its input. When a deadline
/// passes, and when the stream stops, the cancellation token of the call is cancelled.
/// Calls begin on the stage's thread, in the input's order. See the Streams page of the guide.
/// </remarks>
public static class DurableFlow
{
    /// <summary>
    /// Starts one workflow per element. Emits once the start is stored. It does not wait for the workflow to finish.
    /// </summary>
    /// <remarks>
    /// Compute the workflow id from the element. A repeated element then reaches the workflow it started before and
    /// answers <see cref="AlreadyRunning"/>, or <see cref="AlreadyFinished{TOutcome}"/>, and starts no second one.
    /// Output is in the input's order.
    /// </remarks>
    /// <param name="workflows">The workflow type to start.</param>
    /// <param name="plan">The workflow id and the workflow's input for an element.</param>
    /// <param name="parallelism">The most elements in flight.</param>
    /// <param name="timeout">How long one start may take.</param>
    public static Flow<TIn, (TIn Input, Answer<StartResult<TOutcome>> Answer), NotUsed> Start<
        TIn, TInput, TState, TFact, TOutcome>(
        IWorkflowRefs<TInput, TState, TFact, TOutcome> workflows,
        Func<TIn, (WorkflowId Id, TInput Input)> plan,
        int parallelism,
        TimeSpan timeout)
        where TState : notnull
    {
        ArgumentNullException.ThrowIfNull(workflows);
        ArgumentNullException.ThrowIfNull(plan);

        return Flow.Create<TIn>().Via(new DurableStage<TIn, StartResult<TOutcome>>(
            (element, token) =>
            {
                var (id, input) = plan(element);
                return workflows.Get(id).Start(input, token);
            },
            parallelism, timeout, ordered: true));
    }

    /// <summary>
    /// Sends one command per element to an entity, and emits its reply. Output is in the input's order.
    /// </summary>
    /// <remarks>
    /// A timed-out answer does not mean the entity did not handle the command. Send only commands that are safe to
    /// repeat if you send an element again.
    /// </remarks>
    /// <param name="entities">The entity type to send to.</param>
    /// <param name="plan">The entity id and the command for an element. The command's type gives the reply type.</param>
    /// <param name="parallelism">The most elements in flight.</param>
    /// <param name="timeout">How long one command may take.</param>
    public static Flow<TIn, (TIn Input, Answer<TReply> Answer), NotUsed> Ask<TIn, TCommand, TReply>(
        IEntityRefs<TCommand> entities,
        Func<TIn, (EntityId Id, IReplies<TCommand, TReply> Command)> plan,
        int parallelism,
        TimeSpan timeout)
        where TReply : notnull
    {
        ArgumentNullException.ThrowIfNull(entities);
        ArgumentNullException.ThrowIfNull(plan);

        return Flow.Create<TIn>().Via(new DurableStage<TIn, TReply>(
            (element, token) =>
            {
                var (id, command) = plan(element);
                return entities.Get(id).Ask(command, token);
            },
            parallelism, timeout, ordered: true));
    }

    /// <summary>
    /// Sends one fact per element to a running workflow, and emits whether it was taken. Output is in the input's
    /// order.
    /// </summary>
    /// <param name="workflows">The workflow type to send to.</param>
    /// <param name="plan">The workflow id and the fact for an element.</param>
    /// <param name="parallelism">The most elements in flight.</param>
    /// <param name="timeout">How long one delivery may take.</param>
    public static Flow<TIn, (TIn Input, Answer<DeliveryResult<TOutcome>> Answer), NotUsed> Notify<
        TIn, TInput, TState, TFact, TOutcome>(
        IWorkflowRefs<TInput, TState, TFact, TOutcome> workflows,
        Func<TIn, (WorkflowId Id, TFact Fact)> plan,
        int parallelism,
        TimeSpan timeout)
        where TState : notnull
    {
        ArgumentNullException.ThrowIfNull(workflows);
        ArgumentNullException.ThrowIfNull(plan);

        return Flow.Create<TIn>().Via(new DurableStage<TIn, DeliveryResult<TOutcome>>(
            (element, token) =>
            {
                var (id, fact) = plan(element);
                return workflows.Get(id).Notify(fact, token);
            },
            parallelism, timeout, ordered: true));
    }

    /// <summary>
    /// For each element, waits until the entity's state satisfies <paramref name="until"/>, and emits that snapshot.
    /// Emits as each wait ends, not in the input's order.
    /// </summary>
    /// <remarks>
    /// The wait is a subscription, not a poll. It ends when the state shows up, when the deadline passes, and when the
    /// stream stops.
    /// </remarks>
    /// <param name="entities">The entity type to wait on.</param>
    /// <param name="idOf">The entity id for an element.</param>
    /// <param name="until">The condition. It gets the element and each state that reaches the wait.</param>
    /// <param name="parallelism">The most elements in flight.</param>
    /// <param name="timeout">How long one wait may take.</param>
    public static Flow<TIn, (TIn Input, Answer<StateSnapshot<TState>> Answer), NotUsed> Until<
        TIn, TCommand, TState, TEvent>(
        IDurableEntityRefs<TCommand, TState, TEvent> entities,
        Func<TIn, EntityId> idOf,
        Func<TIn, TState, bool> until,
        int parallelism,
        TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(entities);
        ArgumentNullException.ThrowIfNull(idOf);
        ArgumentNullException.ThrowIfNull(until);

        return Flow.Create<TIn>().Via(new DurableStage<TIn, StateSnapshot<TState>>(
            (element, token) => entities.Get(idOf(element)).WaitForStateAsync(state => until(element, state), token),
            parallelism, timeout, ordered: false));
    }

    /// <summary>
    /// For each element, waits until the workflow's status satisfies <paramref name="until"/>, and emits that
    /// snapshot. Emits as each wait ends, not in the input's order.
    /// </summary>
    /// <remarks>
    /// A workflow that was never started has the status <see cref="WorkflowNotStarted"/>. A wait for any other
    /// condition then runs to its deadline, unless <paramref name="until"/> accepts that status.
    /// </remarks>
    /// <param name="workflows">The workflow type to wait on.</param>
    /// <param name="idOf">The workflow id for an element.</param>
    /// <param name="until">The condition. It gets the element and each status that reaches the wait.</param>
    /// <param name="parallelism">The most elements in flight.</param>
    /// <param name="timeout">How long one wait may take.</param>
    public static Flow<TIn, (TIn Input, Answer<StateSnapshot<WorkflowStatus<TState, TOutcome>>> Answer), NotUsed> Until<
        TIn, TInput, TState, TFact, TOutcome>(
        IWorkflowRefs<TInput, TState, TFact, TOutcome> workflows,
        Func<TIn, WorkflowId> idOf,
        Func<TIn, WorkflowStatus<TState, TOutcome>, bool> until,
        int parallelism,
        TimeSpan timeout)
        where TState : notnull
    {
        ArgumentNullException.ThrowIfNull(workflows);
        ArgumentNullException.ThrowIfNull(idOf);
        ArgumentNullException.ThrowIfNull(until);

        return Flow.Create<TIn>().Via(new DurableStage<TIn, StateSnapshot<WorkflowStatus<TState, TOutcome>>>(
            (element, token) => workflows.Get(idOf(element)).WaitForStateAsync(status => until(element, status), token),
            parallelism, timeout, ordered: false));
    }
}
