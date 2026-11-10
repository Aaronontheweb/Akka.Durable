using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Reminders;

namespace Akka.Durable;

/// <summary>
/// <see cref="ActorRegistry"/> key for the shard region or local host of the workflow type whose input is
/// <typeparamref name="TInput"/> and whose outcome is <typeparamref name="TOutcome"/>.
/// </summary>
/// <remarks>
/// One actor system hosts at most one workflow type for a pair of input and outcome types.
/// <c>WithDurableWorkflow</c> rejects a second one.
/// </remarks>
// ReSharper disable once ClassCannotBeInstantiated
public sealed class WorkflowRegion<TInput, TOutcome>
{
    private WorkflowRegion() { }
}

/// <summary>
/// <see cref="ActorRegistry"/> key for the same shard region or local host as
/// <see cref="WorkflowRegion{TInput,TOutcome}"/>, used by refs that also know the fact type.
/// </summary>
// ReSharper disable once ClassCannotBeInstantiated
public sealed class WorkflowRegion<TInput, TFact, TOutcome>
{
    private WorkflowRegion() { }
}

// The answers to Start.
public sealed record Started;
public sealed record AlreadyRunning;
public sealed record AlreadyFinished<TOutcome>(TOutcome Outcome);

/// <summary>The answer to <see cref="IWorkflowRef{TInput,TOutcome}.Start"/>. Only <see cref="Started"/> begins a run.</summary>
public readonly union StartResult<TOutcome>(Started, AlreadyRunning, AlreadyFinished<TOutcome>);

// The conditions a workflow can be in.
public sealed record WorkflowNotStarted;

/// <param name="State">The name of the state the workflow is in.</param>
/// <param name="Since">When it entered that state.</param>
public sealed record WorkflowRunning(string State, DateTimeOffset Since);

public sealed record WorkflowFinished<TOutcome>(TOutcome Outcome, OutcomeKind Kind, DateTimeOffset At);

/// <summary>The condition a workflow is in: not started, running, or finished with its outcome.</summary>
public readonly union WorkflowStatus<TOutcome>(WorkflowNotStarted, WorkflowRunning, WorkflowFinished<TOutcome>);

// The answers to a fact from outside.

/// <summary>The fact was journaled and decided.</summary>
public sealed record Delivered;

/// <summary>The fact was journaled while work was in flight. It is decided when that work settles.</summary>
public sealed record Held;

/// <summary>
/// The answer to <see cref="IWorkflowRef{TInput,TFact,TOutcome}.Notify"/> and
/// <see cref="IWorkflowRef{TInput,TOutcome}.Cancel"/>. A workflow that is not started, or has finished,
/// refuses the fact and journals nothing.
/// </summary>
public readonly union DeliveryResult<TOutcome>(Delivered, Held, WorkflowNotStarted, AlreadyFinished<TOutcome>);

/// <summary>
/// Logical reference to one workflow: its workflow type and <see cref="WorkflowId"/>.
/// </summary>
/// <remarks>
/// The caller chooses the id, from something durable: a domain key, plus an attempt number when the same
/// work can run more than once. A retried <see cref="Start"/> then reaches the same workflow. An id is used
/// once: a finished workflow is closed and cannot be started again.
/// </remarks>
public interface IWorkflowRef<in TInput, TOutcome>
{
    public WorkflowId Id { get; }

    /// <summary>Starts the workflow, if this id has not been started before.</summary>
    Task<StartResult<TOutcome>> Start(TInput input, CancellationToken cancellationToken = default);

    /// <summary>Reads the condition the workflow is in. Answered at once; it never waits for the workflow.</summary>
    Task<WorkflowStatus<TOutcome>> Status(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the status every <paramref name="pollEvery"/> until the workflow has finished, and returns its
    /// outcome. The waiting happens in the caller, not in the workflow. For a workflow that takes seconds.
    /// </summary>
    /// <exception cref="InvalidOperationException">The workflow has not been started.</exception>
    Task<TOutcome> WaitForOutcome(TimeSpan pollEvery, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asks the workflow to stop. The definition's <c>Cancel</c> function turns the request into a fact, and
    /// the table decides what it means in the current state.
    /// </summary>
    /// <param name="reason">Why the workflow should stop.</param>
    Task<DeliveryResult<TOutcome>> Cancel(string reason, CancellationToken cancellationToken = default);
}

/// <summary>A workflow reference that can also send the workflow one of its facts.</summary>
public interface IWorkflowRef<in TInput, in TFact, TOutcome> : IWorkflowRef<TInput, TOutcome>
{
    /// <summary>
    /// Sends the workflow a fact. It is journaled before this returns. While work is in flight it is held,
    /// and decided when that work settles.
    /// </summary>
    Task<DeliveryResult<TOutcome>> Notify(TFact fact, CancellationToken cancellationToken = default);
}

/// <summary>
/// Factory for workflow references of a single workflow type, found by its input and outcome types.
/// </summary>
/// <remarks>Creating a reference is cheap and side-effect free: it does not start or activate the workflow.</remarks>
public interface IWorkflowRefs<in TInput, TOutcome>
{
    IWorkflowRef<TInput, TOutcome> Get(WorkflowId id);
}

/// <summary>
/// Factory for workflow references that can send facts, found by the workflow's input, fact and outcome types.
/// </summary>
public interface IWorkflowRefs<in TInput, in TFact, TOutcome>
{
    IWorkflowRef<TInput, TFact, TOutcome> Get(WorkflowId id);
}

internal sealed class WorkflowRefs<TInput, TOutcome>(IRequiredActor<WorkflowRegion<TInput, TOutcome>> region)
    : IWorkflowRefs<TInput, TOutcome>
    where TInput : notnull
    where TOutcome : notnull
{
    public IWorkflowRef<TInput, TOutcome> Get(WorkflowId id) => new WorkflowRef<TInput, TOutcome>(id, region.ActorRef);
}

internal sealed class WorkflowRefs<TInput, TFact, TOutcome>(IRequiredActor<WorkflowRegion<TInput, TFact, TOutcome>> region)
    : IWorkflowRefs<TInput, TFact, TOutcome>
    where TInput : notnull
    where TFact : notnull
    where TOutcome : notnull
{
    public IWorkflowRef<TInput, TFact, TOutcome> Get(WorkflowId id) =>
        new WorkflowRef<TInput, TFact, TOutcome>(id, region.ActorRef);
}

internal sealed class WorkflowRef<TInput, TFact, TOutcome>(WorkflowId id, IActorRef region)
    : WorkflowRef<TInput, TOutcome>(id, region), IWorkflowRef<TInput, TFact, TOutcome>
    where TInput : notnull
    where TFact : notnull
    where TOutcome : notnull
{
    public async Task<DeliveryResult<TOutcome>> Notify(TFact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);
        return ResultOf(await Entity.Ask(new WfDeliver(fact, WantsReply: true), cancellationToken));
    }
}

internal class WorkflowRef<TInput, TOutcome>(WorkflowId id, IActorRef region) : IWorkflowRef<TInput, TOutcome>
    where TInput : notnull
    where TOutcome : notnull
{
    private protected EntityRef<WfCommand, WfState, WfEvent> Entity { get; } = new(new EntityId(id.Value), region);

    public WorkflowId Id => id;

    public async Task<DeliveryResult<TOutcome>> Cancel(string reason, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reason);
        return ResultOf(await Entity.Ask(new WfCancel(reason, By: null), cancellationToken));
    }

    private protected static DeliveryResult<TOutcome> ResultOf(WfDeliveryReply reply) =>
        reply switch
        {
            WfDelivered => new Delivered(),
            WfHeldReply => new Held(),
            WfNotStartedReply => new WorkflowNotStarted(),
            WfClosedReply closed => new AlreadyFinished<TOutcome>((TOutcome)closed.Outcome),
        };

    public async Task<StartResult<TOutcome>> Start(TInput input, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);

        var reply = await Entity.Ask(new WfStart(input), cancellationToken);
        return reply switch
        {
            WfStartAccepted => new Started(),
            WfStartRefusedRunning => new AlreadyRunning(),
            WfStartRefusedFinished finished => new AlreadyFinished<TOutcome>((TOutcome)finished.Outcome),
        };
    }

    public async Task<WorkflowStatus<TOutcome>> Status(CancellationToken cancellationToken = default)
    {
        var snapshot = await Entity.GetStateAsync(cancellationToken);
        return snapshot.State switch
        {
            WfNotStarted => new WorkflowNotStarted(),
            WfRunning running => new WorkflowRunning(NameOf(running.State), running.EnteredAt),
            WfFinished finished => new WorkflowFinished<TOutcome>((TOutcome)finished.Outcome, finished.Kind, finished.At),
        };
    }

    public async Task<TOutcome> WaitForOutcome(TimeSpan pollEvery, CancellationToken cancellationToken = default)
    {
        if (pollEvery <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(pollEvery), pollEvery, "The poll interval must be positive.");

        while (true)
        {
            switch (await Status(cancellationToken))
            {
                case WorkflowFinished<TOutcome> finished:
                    return finished.Outcome;

                case WorkflowNotStarted:
                    throw new InvalidOperationException(
                        $"Workflow [{id.Value}] has not been started, so it has no outcome to wait for. Call Start first.");
            }

            await Task.Delay(pollEvery, cancellationToken);
        }
    }

    /// <summary>The name of the state's type. For a union, the name of the case it holds.</summary>
    private static string NameOf(object state) =>
        (state is IUnion { Value: { } value } ? value : state).GetType().Name;
}

public static class DurableWorkflowHostingExtensions
{
    /// <summary>
    /// Hosts a workflow type: in a shard region when <paramref name="sharding"/> is given, otherwise in a
    /// local host actor in this process. Call <c>AddDurableEntities</c> on the service collection as well.
    /// </summary>
    public static AkkaConfigurationBuilder WithDurableWorkflow<TInput, TState, TFact, TOutcome>(
        this AkkaConfigurationBuilder builder,
        WorkflowDefinition<TInput, TState, TFact, TOutcome> definition,
        ShardedHosting? sharding = null)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        var entity = new WorkflowEntity<TInput, TState, TFact, TOutcome>(definition).Compile();

        // Refs find the workflow type by its input and outcome types, so two workflow types cannot share them.
        var key = typeof(WorkflowRegion<TInput, TOutcome>);
        if (DurableEntityHostingExtensions.ClaimRegionKey(builder, entity.TypeName, key) is { } owner)
            throw new InvalidOperationException(owner == entity.TypeName
                ? $"Durable workflow [{entity.TypeName.Value}] is registered twice. Call WithDurableWorkflow once for each workflow type."
                : $"Durable workflows [{owner.Value}] and [{entity.TypeName.Value}] both have the input type {typeof(TInput).FullName} " +
                  $"and the outcome type {typeof(TOutcome).FullName}. Each workflow type needs its own input type, " +
                  "because workflow refs find the workflow type by it.");

        // The author's types are carried inside the library's own, so they are checked here by name.
        builder.WithActors((system, _) => DurableEntityHostingExtensions.EnsureSerializable(
            system, "Durable workflow", entity.TypeName, typeof(TInput), typeof(TState), typeof(TFact), typeof(TOutcome)));

        // Every workflow stores a reminder that carries a WfCommand. Without this, Native AOT cannot build its envelope.
        ReminderMessageTypes.Register<WfCommand>();

        // Refs that can send facts find the same actor under a key that also names the fact type.
        return builder.HostEntity<WfState, WfCommand, WfEvent, WorkflowRegion<TInput, TOutcome>>(entity, sharding,
            (registry, region) => registry.Register<WorkflowRegion<TInput, TFact, TOutcome>>(region));
    }
}
