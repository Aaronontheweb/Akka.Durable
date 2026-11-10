using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Hosting;

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
}

/// <summary>
/// Factory for workflow references of a single workflow type, found by its input and outcome types.
/// </summary>
/// <remarks>Creating a reference is cheap and side-effect free: it does not start or activate the workflow.</remarks>
public interface IWorkflowRefs<in TInput, TOutcome>
{
    IWorkflowRef<TInput, TOutcome> Get(WorkflowId id);
}

internal sealed class WorkflowRefs<TInput, TOutcome>(IRequiredActor<WorkflowRegion<TInput, TOutcome>> region)
    : IWorkflowRefs<TInput, TOutcome>
    where TInput : notnull
    where TOutcome : notnull
{
    public IWorkflowRef<TInput, TOutcome> Get(WorkflowId id) => new WorkflowRef<TInput, TOutcome>(id, region.ActorRef);
}

internal sealed class WorkflowRef<TInput, TOutcome>(WorkflowId id, IActorRef region) : IWorkflowRef<TInput, TOutcome>
    where TInput : notnull
    where TOutcome : notnull
{
    private readonly EntityRef<WfCommand, WfState, WfEvent> _entity = new(new EntityId(id.Value), region);

    public WorkflowId Id => id;

    public async Task<StartResult<TOutcome>> Start(TInput input, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);

        var reply = await _entity.Ask(new WfStart(input), cancellationToken);
        return reply switch
        {
            WfStartAccepted => new Started(),
            WfStartRefusedRunning => new AlreadyRunning(),
            WfStartRefusedFinished finished => new AlreadyFinished<TOutcome>((TOutcome)finished.Outcome),
        };
    }

    public async Task<WorkflowStatus<TOutcome>> Status(CancellationToken cancellationToken = default)
    {
        var snapshot = await _entity.GetStateAsync(cancellationToken);
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

        return builder.HostEntity<WfState, WfCommand, WfEvent, WorkflowRegion<TInput, TOutcome>>(entity, sharding);
    }
}
