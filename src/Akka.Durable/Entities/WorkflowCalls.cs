using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

/// <summary>
/// The effect of <c>Start</c> on a decision: start the workflow <see cref="Id"/> of the workflow type
/// <see cref="Workflow"/> with <see cref="Input"/>. It is the <see cref="ExecuteEffect.Effect"/> of an
/// <see cref="ExecuteEffect"/>.
/// </summary>
/// <remarks>
/// The start is journaled in the same write as the decision's events, and the library delivers it until the workflow
/// answers, also after a restart. A start is addressed by id, so a repeat is answered "already running" or "already
/// finished".
/// </remarks>
public sealed record StartWorkflow(WorkflowTypeName Workflow, WorkflowId Id, object Input) : IWorkflowCall
{
    Type IWorkflowCall.RegionKey => RegionKey;
    internal Type RegionKey { get; init; } = typeof(object);

    object IWorkflowCall.Recorded => new RecordedStart(Workflow.Value, Id.Value, Input);

    public bool Equals(StartWorkflow? other) =>
        other is not null && Workflow == other.Workflow && Id == other.Id && Equals(Input, other.Input);

    public override int GetHashCode() => HashCode.Combine(Workflow, Id, Input);
}

/// <summary>
/// The effect of <c>Notify</c> on a decision: send <see cref="Fact"/> to the workflow <see cref="Id"/> of the workflow
/// type <see cref="Workflow"/>. Journaled with the decision, attempted once. It is the <see cref="ExecuteEffect.Effect"/>
/// of an <see cref="ExecuteEffect"/>.
/// </summary>
public sealed record NotifyWorkflow(WorkflowTypeName Workflow, WorkflowId Id, object Fact) : IWorkflowCall
{
    Type IWorkflowCall.RegionKey => RegionKey;
    internal Type RegionKey { get; init; } = typeof(object);

    object IWorkflowCall.Recorded => new RecordedNotify(Workflow.Value, Id.Value, Fact);

    public bool Equals(NotifyWorkflow? other) =>
        other is not null && Workflow == other.Workflow && Id == other.Id && Equals(Fact, other.Fact);

    public override int GetHashCode() => HashCode.Combine(Workflow, Id, Fact);
}

/// <summary>
/// The effect of <c>Cancel</c> on a decision: ask the workflow <see cref="Id"/> of the workflow type
/// <see cref="Workflow"/> to stop. Journaled with the decision, attempted once. It is the
/// <see cref="ExecuteEffect.Effect"/> of an <see cref="ExecuteEffect"/>.
/// </summary>
public sealed record CancelWorkflow(WorkflowTypeName Workflow, WorkflowId Id, string Reason, string By) : IWorkflowCall
{
    Type IWorkflowCall.RegionKey => RegionKey;
    internal Type RegionKey { get; init; } = typeof(object);

    object IWorkflowCall.Recorded => new RecordedCancel(Workflow.Value, Id.Value, Reason, By);

    public bool Equals(CancelWorkflow? other) =>
        other is not null && Workflow == other.Workflow && Id == other.Id && Reason == other.Reason && By == other.By;

    public override int GetHashCode() => HashCode.Combine(Workflow, Id, Reason, By);
}

/// <summary>A call from an entity to a workflow. The entity checks it against the workflows its definition uses.</summary>
internal interface IWorkflowCall
{
    WorkflowTypeName Workflow { get; }
    WorkflowId Id { get; }

    /// <summary>The <see cref="WorkflowRegion{TInput,TState,TFact,TOutcome}"/> type of the workflow's definition.</summary>
    Type RegionKey { get; }

    /// <summary>The call as it is journaled.</summary>
    object Recorded { get; }
}

/// <summary>
/// A workflow type an entity may call: declared with <c>Uses</c>. It holds, bound to the workflow's types, how to
/// deliver each call, and how the answer and the give-up of a <c>Notify</c> or <c>Cancel</c> become the entity's
/// commands. It lives on the definition, so it is there after a restart.
/// </summary>
/// <param name="Workflow">The workflow type's name.</param>
/// <param name="RegionKey">The <see cref="WorkflowRegion{TInput,TState,TFact,TOutcome}"/> type it is hosted under.</param>
/// <param name="InputType">The workflow's input type.</param>
/// <param name="Start">Sends a start.</param>
/// <param name="Notify">Sends a fact and returns the command its answer becomes. Null without <c>told</c>.</param>
/// <param name="Cancel">Sends a cancel and returns the command its answer becomes. Null without <c>told</c>.</param>
/// <param name="GaveUp">The command a notify or cancel with no answer becomes. Null without <c>gaveUp</c>.</param>
internal sealed record WorkflowUse(
    WorkflowTypeName Workflow,
    Type RegionKey,
    Type InputType,
    Func<IServiceProvider, WorkflowId, object, CancellationToken, Task> Start,
    Func<IServiceProvider, WorkflowId, object, CancellationToken, Task<object?>>? Notify,
    Func<IServiceProvider, WorkflowId, string, string, CancellationToken, Task<object?>>? Cancel,
    Func<WorkGaveUp, object?>? GaveUp)
{
    /// <summary>The policy of every start: it is delivered until the workflow answers.</summary>
    public static readonly RetryPolicy StartPolicy = Retry.UntilAcknowledged(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1));

    public static WorkflowUse Of<TCommand, TInput, TState, TFact, TOutcome>(
        WorkflowDefinition<TInput, TState, TFact, TOutcome> definition,
        Func<DeliveryResult<TOutcome>, TCommand>? told,
        Func<WorkGaveUp, TCommand>? gaveUp)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        var name = definition.TypeName;
        return new WorkflowUse(name, typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>), typeof(TInput),
            Start: async (services, id, input, ct) =>
                await Refs<TInput, TState, TFact, TOutcome>(services, name).Get(id).Start((TInput)input, ct),
            Notify: told is null ? null : async (services, id, fact, ct) =>
                told(await Refs<TInput, TState, TFact, TOutcome>(services, name).Get(id).Notify((TFact)fact, ct)),
            Cancel: told is null ? null : async (services, id, reason, by, ct) =>
                told(await Refs<TInput, TState, TFact, TOutcome>(services, name).Get(id).Cancel(reason, by, ct)),
            GaveUp: gaveUp is null ? null : failure => gaveUp(failure));
    }

    public bool Matches(IWorkflowCall call) => call.Workflow == Workflow && call.RegionKey == RegionKey;

    /// <summary>True when the entity is told the answer of a notify or cancel, and its give-up.</summary>
    public bool Answers => Notify is not null && GaveUp is not null;

    /// <summary>Resolved when the call runs, so a missing registration fails the call instead of the entity.</summary>
    private static IWorkflowRefs<TInput, TState, TFact, TOutcome> Refs<TInput, TState, TFact, TOutcome>(
        IServiceProvider services, WorkflowTypeName workflow)
        where TState : notnull =>
        services.GetService<IWorkflowRefs<TInput, TState, TFact, TOutcome>>()
        ?? throw new InvalidOperationException(
            $"No workflow refs are registered, so workflow [{workflow.Value}] cannot be called. " +
            "Call AddDurableEntities() on the service collection.");
}

internal static class WorkflowCalls
{
    public static ExecuteEffect Start<TInput, TState, TFact, TOutcome>(WorkflowTypeName workflow, WorkflowId id, TInput input)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull =>
        new(new StartWorkflow(workflow, id, input) { RegionKey = typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>) })
        {
            Recorded = true,
        };

    public static ExecuteEffect Notify<TInput, TState, TFact, TOutcome>(WorkflowTypeName workflow, WorkflowId id, TFact fact)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull =>
        new(new NotifyWorkflow(workflow, id, fact) { RegionKey = typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>) })
        {
            Recorded = true,
        };

    public static ExecuteEffect Cancel<TInput, TState, TFact, TOutcome>(
        WorkflowTypeName workflow, WorkflowId id, string reason, string by)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull =>
        new(new CancelWorkflow(workflow, id, reason, by) { RegionKey = typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>) })
        {
            Recorded = true,
        };
}
