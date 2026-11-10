using Akka.Serialization.V2;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

/// <summary>
/// The effect of <c>Start</c> on a decision: start the workflow <see cref="Id"/> of the workflow type
/// <see cref="Workflow"/> with <see cref="Input"/>. It is the <see cref="ExecuteEffect.Effect"/> of an
/// <see cref="ExecuteEffect"/>.
/// </summary>
/// <remarks>
/// The library completes the start: until the workflow answers, it stores a durable reminder that issues the start
/// again, also after a restart. A start is addressed by id, so a repeat is answered "already running" or
/// "already finished".
/// </remarks>
public sealed record StartWorkflow(WorkflowTypeName Workflow, WorkflowId Id, object Input) : IWorkflowCall
{
    Type IWorkflowCall.RegionKey => RegionKey;
    internal Type RegionKey { get; init; } = typeof(object);

    public bool Equals(StartWorkflow? other) =>
        other is not null && Workflow == other.Workflow && Id == other.Id && Equals(Input, other.Input);

    public override int GetHashCode() => HashCode.Combine(Workflow, Id, Input);
}

/// <summary>
/// The effect of <c>Notify</c> on a decision: send <see cref="Fact"/> to the workflow <see cref="Id"/> of the workflow
/// type <see cref="Workflow"/>. Sent once. It is the <see cref="ExecuteEffect.Effect"/> of an <see cref="ExecuteEffect"/>.
/// </summary>
public sealed record NotifyWorkflow(WorkflowTypeName Workflow, WorkflowId Id, object Fact) : IWorkflowCall
{
    Type IWorkflowCall.RegionKey => RegionKey;
    internal Type RegionKey { get; init; } = typeof(object);

    public bool Equals(NotifyWorkflow? other) =>
        other is not null && Workflow == other.Workflow && Id == other.Id && Equals(Fact, other.Fact);

    public override int GetHashCode() => HashCode.Combine(Workflow, Id, Fact);
}

/// <summary>
/// The effect of <c>Cancel</c> on a decision: ask the workflow <see cref="Id"/> of the workflow type
/// <see cref="Workflow"/> to stop. Sent once. It is the <see cref="ExecuteEffect.Effect"/> of an <see cref="ExecuteEffect"/>.
/// </summary>
public sealed record CancelWorkflow(WorkflowTypeName Workflow, WorkflowId Id, string Reason, string By) : IWorkflowCall
{
    Type IWorkflowCall.RegionKey => RegionKey;
    internal Type RegionKey { get; init; } = typeof(object);

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
}

/// <summary>A workflow type an entity may call: declared with <c>Uses</c>.</summary>
/// <param name="Workflow">The workflow type's name.</param>
/// <param name="RegionKey">The <see cref="WorkflowRegion{TInput,TState,TFact,TOutcome}"/> type it is hosted under.</param>
/// <param name="Restart">
/// Builds the start again from a <see cref="PendingStart"/>, with no answer to deliver. Null when the stored input is
/// not of the workflow's input type.
/// </param>
internal sealed record WorkflowUse(WorkflowTypeName Workflow, Type RegionKey, Func<WorkflowId, object, ExecuteEffect?> Restart)
{
    public static WorkflowUse Of<TInput, TState, TFact, TOutcome>(WorkflowDefinition<TInput, TState, TFact, TOutcome> definition)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull =>
        new(definition.TypeName, typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>),
            (id, input) => input is TInput typed
                ? WorkflowCalls.Start<TInput, TState, TFact, TOutcome>(definition.TypeName, id, typed, then: null, gaveUp: null)
                : null);

    public bool Matches(IWorkflowCall call) => call.Workflow == Workflow && call.RegionKey == RegionKey;
}

/// <summary>
/// The payload of the reminder that keeps a start the workflow has not answered yet. When it fires, the entity
/// issues the start again.
/// </summary>
/// <param name="Workflow">The workflow type's name.</param>
/// <param name="Id">The workflow's id.</param>
/// <param name="Input">The author's input, written with the serializer bound to its type.</param>
[AkkaSerializable(Manifest = "durable-pending-start-v1")]
internal sealed record PendingStart(
    [property: AkkaField(0)] string Workflow,
    [property: AkkaField(1)] string Id,
    [property: AkkaField(2)] object Input) : IDurableEntityProtocol
{
    /// <summary>
    /// The reminder's key. One per workflow an entity starts, so a second start of the same workflow replaces it.
    /// </summary>
    public static string KeyOf(WorkflowTypeName workflow, WorkflowId id) => $"durable-start:{workflow.Value}:{id.Value}";

    public string Key => KeyOf(new WorkflowTypeName(Workflow), new WorkflowId(Id));
}

/// <summary>A decision calls a workflow type that the entity's definition does not name with <c>Uses</c>.</summary>
internal sealed class UndeclaredWorkflowException(EntityTypeName entity, WorkflowTypeName workflow) : InvalidOperationException(
    $"Entity type [{entity.Value}] called workflow type [{workflow.Value}], which its definition does not name. " +
    $"Declare it with .Uses(...) on the entity definition, passing the definition of [{workflow.Value}].");

internal static class WorkflowCalls
{
    /// <summary>
    /// A start. With <paramref name="then"/> the answer becomes one of the entity's commands; without it the answer
    /// only ends the repeats.
    /// </summary>
    public static ExecuteEffect Start<TInput, TState, TFact, TOutcome>(
        WorkflowTypeName workflow, WorkflowId id, TInput input,
        Func<StartResult<TOutcome>, object?>? then, Func<WorkGaveUp, object?>? gaveUp)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        var effect = new StartWorkflow(workflow, id, input) { RegionKey = typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>) };
        return new ExecuteEffect(effect)
        {
            Resolve = services => async ct =>
            {
                var answer = await Refs<TInput, TState, TFact, TOutcome>(services, workflow).Get(id).Start(input, ct);
                return then?.Invoke(answer);
            },
            Failed = gaveUp is null ? null : failure => gaveUp(GaveUp(effect, failure)),
        };
    }

    public static ExecuteEffect Notify<TInput, TState, TFact, TOutcome>(
        WorkflowTypeName workflow, WorkflowId id, TFact fact,
        Func<DeliveryResult<TOutcome>, object?> then, Func<WorkGaveUp, object?> gaveUp)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        var effect = new NotifyWorkflow(workflow, id, fact) { RegionKey = typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>) };
        return new ExecuteEffect(effect)
        {
            Resolve = services => async ct =>
                then(await Refs<TInput, TState, TFact, TOutcome>(services, workflow).Get(id).Notify(fact, ct)),
            Failed = failure => gaveUp(GaveUp(effect, failure)),
        };
    }

    public static ExecuteEffect Cancel<TInput, TState, TFact, TOutcome>(
        WorkflowTypeName workflow, WorkflowId id, string reason, string by,
        Func<DeliveryResult<TOutcome>, object?> then, Func<WorkGaveUp, object?> gaveUp)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        var effect = new CancelWorkflow(workflow, id, reason, by) { RegionKey = typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>) };
        return new ExecuteEffect(effect)
        {
            Resolve = services => async ct =>
                then(await Refs<TInput, TState, TFact, TOutcome>(services, workflow).Get(id).Cancel(reason, by, ct)),
            Failed = failure => gaveUp(GaveUp(effect, failure)),
        };
    }

    private static WorkGaveUp GaveUp(object effect, Exception failure) =>
        new(effect, failure is TimeoutException ? GaveUpReason.TimedOut : GaveUpReason.Failed, failure.Message);

    /// <summary>Resolved when the call runs, so a missing registration fails the call instead of the entity.</summary>
    private static IWorkflowRefs<TInput, TState, TFact, TOutcome> Refs<TInput, TState, TFact, TOutcome>(
        IServiceProvider services, WorkflowTypeName workflow)
        where TState : notnull =>
        services.GetService<IWorkflowRefs<TInput, TState, TFact, TOutcome>>()
        ?? throw new InvalidOperationException(
            $"No workflow refs are registered, so workflow [{workflow.Value}] cannot be called. " +
            "Call AddDurableEntities() on the service collection.");
}
