using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Hosting;
using Akka.Reminders;

namespace Akka.Durable;

/// <summary>
/// <see cref="ActorRegistry"/> key for the shard region or local host of the workflow type whose input is
/// <typeparamref name="TInput"/>, whose state is <typeparamref name="TState"/>, whose fact is
/// <typeparamref name="TFact"/> and whose outcome is <typeparamref name="TOutcome"/>.
/// </summary>
/// <remarks>
/// One actor system hosts at most one workflow type for each set of input, state, fact and outcome types.
/// <c>WithDurableWorkflow</c> rejects a second one. Types that match no hosted workflow type have no registry
/// entry, so <see cref="IWorkflowRefs{TInput,TState,TFact,TOutcome}.Get"/> throws for them.
/// </remarks>
// ReSharper disable once ClassCannotBeInstantiated
public sealed class WorkflowRegion<TInput, TState, TFact, TOutcome>
{
    private WorkflowRegion() { }
}

// The answers to Start.
public sealed record Started;
public sealed record AlreadyRunning;
public sealed record AlreadyFinished<TOutcome>(TOutcome Outcome);

/// <summary>The answer to <see cref="IWorkflowRef{TInput,TState,TFact,TOutcome}.Start"/>. Only <see cref="Started"/> begins a run.</summary>
public readonly union StartResult<TOutcome>(Started, AlreadyRunning, AlreadyFinished<TOutcome>);

// The conditions a workflow can be in.
public sealed record WorkflowNotStarted;

/// <summary>The workflow is running. It carries the state the workflow is in, with the type the definition gives it.</summary>
/// <typeparam name="TState">The workflow's state type, the same type as in its definition.</typeparam>
/// <param name="State">
/// The state the workflow is in, typed as the author typed it. When the state type is a C# union, this is the
/// union value itself, not the case it holds. The compiler then checks a <c>switch</c> expression over it: a
/// state that is added later and that the <c>switch</c> does not handle is reported (CS8509), so no catch-all
/// arm is needed.
/// </param>
/// <param name="Since">When it entered that state.</param>
public sealed record WorkflowRunning<TState>(TState State, DateTimeOffset Since)
    where TState : notnull
{
    /// <summary>
    /// The name of the record the state holds. For a union, the name of the case it holds, also when that case
    /// is a union itself. Use it for logs and simple displays. To read the state's data, match on
    /// <see cref="State"/>.
    /// </summary>
    public string StateName => Unwrap(State).GetType().Name;

    /// <summary>The record a state holds. A union gives the case it holds, also when that case is a union itself.</summary>
    private static object Unwrap(object state)
    {
        while (state is IUnion { Value: { } value })
            state = value;
        return state;
    }
}

public sealed record WorkflowFinished<TOutcome>(TOutcome Outcome, DateTimeOffset At);

/// <summary>
/// The condition a workflow is in: not started, running in a state of type <typeparamref name="TState"/>, or
/// finished with its outcome.
/// </summary>
public readonly union WorkflowStatus<TState, TOutcome>(WorkflowNotStarted, WorkflowRunning<TState>, WorkflowFinished<TOutcome>)
    where TState : notnull;

// The answers to a fact from outside.

/// <summary>The fact was journaled and decided.</summary>
public sealed record Delivered;

/// <summary>The fact was journaled while work was in flight. It is decided when that work settles.</summary>
public sealed record Held;

/// <summary>
/// The answer to <see cref="IWorkflowRef{TInput,TState,TFact,TOutcome}.Notify"/> and
/// <see cref="IWorkflowRef{TInput,TState,TFact,TOutcome}.Cancel"/>. A workflow that is not started, or has finished,
/// refuses the fact and journals nothing.
/// </summary>
public readonly union DeliveryResult<TOutcome>(Delivered, Held, WorkflowNotStarted, AlreadyFinished<TOutcome>);

/// <summary>
/// Logical reference to one workflow: its workflow type and <see cref="WorkflowId"/>.
/// </summary>
/// <remarks>
/// <para>
/// The type arguments are those of the workflow's <see cref="WorkflowDefinition{TInput,TState,TFact,TOutcome}"/>,
/// in the same order. The ref knows the state type, so <see cref="Status"/> returns the state typed.
/// </para>
/// <para>
/// The caller chooses the id, from something durable: a domain key, plus an attempt number when the same
/// work can run more than once. A retried <see cref="Start"/> then reaches the same workflow. An id is used
/// once: a finished workflow is closed and cannot be started again.
/// </para>
/// </remarks>
public interface IWorkflowRef<in TInput, TState, in TFact, TOutcome>
    where TState : notnull
{
    public WorkflowId Id { get; }

    /// <summary>Starts the workflow, if this id has not been started before.</summary>
    Task<StartResult<TOutcome>> Start(TInput input, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the condition the workflow is in. Answered at once; it never waits for the workflow. A running
    /// workflow gives its state as a <typeparamref name="TState"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The journal holds a state that is not a <typeparamref name="TState"/>, for example because the
    /// definition's state type changed after the workflow was started.
    /// </exception>
    Task<WorkflowStatus<TState, TOutcome>> Status(CancellationToken cancellationToken = default);

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
    /// <param name="reason">Why the workflow should stop. Required, and not empty.</param>
    /// <param name="by">
    /// Who or what asked for the cancel. Required, and not empty. Use an id, such as a user id or a service name,
    /// not a person's name or e-mail address, because the value goes into the journal and the log.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="reason"/> or <paramref name="by"/> is null or empty.</exception>
    Task<DeliveryResult<TOutcome>> Cancel(string reason, string by, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends the workflow a fact. It is journaled before this returns. While work is in flight it is held,
    /// and decided when that work settles.
    /// </summary>
    Task<DeliveryResult<TOutcome>> Notify(TFact fact, CancellationToken cancellationToken = default);
}

/// <summary>
/// Factory for workflow references of a single workflow type, found by its input, state, fact and outcome types.
/// </summary>
/// <remarks>Creating a reference is cheap and side-effect free: it does not start or activate the workflow.</remarks>
public interface IWorkflowRefs<in TInput, TState, in TFact, TOutcome>
    where TState : notnull
{
    /// <summary>Gets the ref for one workflow.</summary>
    /// <exception cref="InvalidOperationException">
    /// No workflow type with these four types is hosted in this actor system. This is also the error when
    /// <typeparamref name="TState"/> is not the state type of the hosted workflow's definition.
    /// </exception>
    IWorkflowRef<TInput, TState, TFact, TOutcome> Get(WorkflowId id);
}

internal sealed class WorkflowRefs<TInput, TState, TFact, TOutcome>(
    IRequiredActor<WorkflowRegion<TInput, TState, TFact, TOutcome>> region)
    : IWorkflowRefs<TInput, TState, TFact, TOutcome>
    where TInput : notnull
    where TState : notnull
    where TFact : notnull
    where TOutcome : notnull
{
    public IWorkflowRef<TInput, TState, TFact, TOutcome> Get(WorkflowId id)
    {
        IActorRef actor;
        try
        {
            actor = region.ActorRef;
        }
        catch (MissingActorRegistryEntryException error)
        {
            throw new InvalidOperationException(
                $"No durable workflow type with the input type {typeof(TInput).FullName}, the state type " +
                $"{typeof(TState).FullName}, the fact type {typeof(TFact).FullName} and the outcome type " +
                $"{typeof(TOutcome).FullName} is hosted in this ActorSystem. Workflow refs find the workflow type by " +
                "these four types, so the state type must be the one in the definition passed to WithDurableWorkflow.",
                error);
        }

        return new WorkflowRef<TInput, TState, TFact, TOutcome>(id, actor);
    }
}

internal sealed class WorkflowRef<TInput, TState, TFact, TOutcome>(WorkflowId id, IActorRef region)
    : IWorkflowRef<TInput, TState, TFact, TOutcome>
    where TInput : notnull
    where TState : notnull
    where TFact : notnull
    where TOutcome : notnull
{
    private EntityRef<WfCommand, WfState, WfEvent> Entity { get; } = new(new EntityId(id.Value), region);

    public WorkflowId Id => id;

    public async Task<DeliveryResult<TOutcome>> Notify(TFact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);
        return ResultOf(await Entity.Ask(new WfDeliver(fact, WantsReply: true), cancellationToken));
    }

    public async Task<DeliveryResult<TOutcome>> Cancel(string reason, string by, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(reason);
        ArgumentException.ThrowIfNullOrEmpty(by);
        return ResultOf(await Entity.Ask(new WfCancel(reason, by), cancellationToken));
    }

    private static DeliveryResult<TOutcome> ResultOf(WfDeliveryReply reply) =>
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

    public async Task<WorkflowStatus<TState, TOutcome>> Status(CancellationToken cancellationToken = default)
    {
        var snapshot = await Entity.GetStateAsync(cancellationToken);
        return snapshot.State switch
        {
            WfNotStarted => new WorkflowNotStarted(),
            WfRunning running => new WorkflowRunning<TState>(StateOf(running), running.EnteredAt),
            WfFinished finished => new WorkflowFinished<TOutcome>((TOutcome)finished.Outcome, finished.At),
        };
    }

    /// <summary>
    /// The author's state as a <typeparamref name="TState"/>. The registry key holds the state type, so this ref
    /// only reaches a workflow type with this state type. The check catches a journal written with another one.
    /// </summary>
    private TState StateOf(WfRunning running) =>
        running.State is TState state
            ? state
            : throw new InvalidOperationException(
                $"Workflow [{id.Value}] holds a state of type {running.State.GetType().FullName}, but its ref reads " +
                $"the state type {typeof(TState).FullName}. The journal was written with another state type.");

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

        // Refs find the workflow type by its input, state, fact and outcome types, so two workflow types cannot
        // share all four.
        var key = typeof(WorkflowRegion<TInput, TState, TFact, TOutcome>);
        if (DurableEntityHostingExtensions.ClaimRegionKey(builder, entity.TypeName, key) is { } owner)
            throw new InvalidOperationException(owner == entity.TypeName
                ? $"Durable workflow [{entity.TypeName.Value}] is registered twice. Call WithDurableWorkflow once for each workflow type."
                : $"Durable workflows [{owner.Value}] and [{entity.TypeName.Value}] both have the input type {typeof(TInput).FullName}, " +
                  $"the state type {typeof(TState).FullName}, the fact type {typeof(TFact).FullName} and the outcome " +
                  $"type {typeof(TOutcome).FullName}. Each workflow type needs its own set of these four types, because " +
                  "workflow refs find the workflow type by them.");

        // The author's types are carried inside the library's own, so they are checked here by name.
        builder.WithActors((system, _) => DurableEntityHostingExtensions.EnsureSerializable(
            system, "Durable workflow", entity.TypeName, typeof(TInput), typeof(TState), typeof(TFact), typeof(TOutcome)));

        // Every workflow stores a reminder that carries a WfCommand. Without this, Native AOT cannot build its envelope.
        ReminderMessageTypes.Register<WfCommand>();

        return builder.HostEntity<WfState, WfCommand, WfEvent, WorkflowRegion<TInput, TState, TFact, TOutcome>>(entity, sharding);
    }
}
