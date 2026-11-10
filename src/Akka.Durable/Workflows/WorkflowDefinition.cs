using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

/// <summary>
/// Name of a workflow type. It is also the name of the entity type that hosts the workflow, so it names
/// the shard region and starts every persistence id.
/// </summary>
public readonly record struct WorkflowTypeName(string Value);

/// <summary>What kind of ending an outcome is.</summary>
public enum OutcomeKind
{
    Succeeded = 0,
    Failed = 1,
    Cancelled = 2,
}

/// <summary>Why a piece of work got no answer.</summary>
public enum GaveUpReason
{
    /// <summary>The handler threw, or no handler is registered for the effect.</summary>
    Failed = 0,

    /// <summary>The handler did not answer within <see cref="WorkflowOptions.AttemptLimit"/>.</summary>
    TimedOut = 1,

    /// <summary>
    /// The workflow restarted after it entered the state and before the answer arrived. The work may have
    /// run, may have run in part, or may not have run.
    /// </summary>
    Interrupted = 2,
}

/// <summary>
/// The notice that a piece of work got no answer. The workflow's <c>GaveUp</c> function turns it into one of
/// the workflow's own facts, and the fact table decides what it means.
/// </summary>
/// <param name="Work">The effect record that was issued.</param>
/// <param name="Reason">Why there is no answer.</param>
/// <param name="Error">The failure's message, when there was one.</param>
/// <remarks>
/// No answer does not mean the work did not happen: a charge can go through and its reply be lost. Work is
/// not issued again on its own, so the table chooses: ask again with the same key, look the result up, wait
/// for the service to report it, or ask a person.
/// </remarks>
public sealed record WorkGaveUp(object Work, GaveUpReason Reason, string? Error = null)
{
    /// <summary>The name of the effect type, for a message or a log line.</summary>
    public string What => Work.GetType().Name;
}

/// <summary>The request to stop a workflow. The definition's <c>Cancel</c> function turns it into a fact.</summary>
/// <param name="Reason">Why the workflow should stop.</param>
/// <param name="By">Who asked, when that is known.</param>
public sealed record CancelRequest(string Reason, string? By = null);

public sealed record WorkflowOptions
{
    /// <summary>
    /// How long one attempt at a piece of work may take. After that its result counts as unknown and the
    /// workflow receives the give-up fact. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan AttemptLimit { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The settings of the entity type that hosts the workflow: shards, idle passivation, persistence id
    /// and plugins. <see cref="EntityOptions.EffectTimeout"/> is replaced by <see cref="AttemptLimit"/>.
    /// </summary>
    public EntityOptions Hosting { get; init; } = EntityOptions.Default;

    public static readonly WorkflowOptions Default = new();
}

/// <summary>
/// A finite process that survives restarts: it starts once, moves through the author's states, and ends
/// once with an outcome.
/// </summary>
/// <param name="TypeName">The workflow type's name.</param>
/// <param name="Start">The state a new workflow begins in.</param>
/// <param name="Work">
/// The work a state needs. A pure function of the state: it is called again whenever the library needs to
/// know what a state's work is, so it must give the same answer every time.
/// </param>
/// <param name="Decide">What a fact means in a state. One table for everything that arrives.</param>
/// <param name="Kind">What kind of ending an outcome is.</param>
/// <param name="Cancel">
/// The fact a cancel arrives as. The table decides what a cancel means in each state.
/// </param>
/// <param name="GaveUp">The fact that says a piece of work got no answer.</param>
/// <param name="Options">Optional settings.</param>
/// <typeparam name="TInput">The record the workflow starts with. Available in every state.</typeparam>
/// <typeparam name="TState">The states, typically a union of records.</typeparam>
/// <typeparam name="TFact">Everything that can arrive, typically a union. Facts are what the journal records.</typeparam>
/// <typeparam name="TOutcome">The ways the workflow can end, typically a union.</typeparam>
/// <remarks>
/// <para>Work is issued once. When a piece of work gets no answer, because its handler failed, ran out of
/// time, or the workflow restarted in the middle, the library does not issue it again. It sends the fact
/// that <paramref name="GaveUp"/> names and the table decides.</para>
/// <para>Facts can also come from outside: a ref's <c>Notify</c> and <c>Cancel</c>, or a message of a type
/// named by <see cref="Accepts{TMessage}"/>. A fact from outside is journaled before the caller gets its
/// reply. While the work of the current state is in flight, it is held. When the work settles, the work's
/// own fact is decided first, then each held fact in the order it arrived, and only then is the work of the
/// state that follows issued.</para>
/// <para>A state can also wait for a point in time with <c>ctx.Timer</c>. A wait is not work: it holds nothing,
/// it never gives up, and it can last months. One durable reminder for each workflow wakes it when a timer is
/// due, and also when work was interrupted by a crash, so the give-up fact arrives without another message.
/// The decision is journaled first; the reminder is stored next, before any work is issued or reply sent.
/// When it cannot be stored, no work is issued; recovery stores it again.</para>
/// <para>The workflow is hosted as a durable entity. All four types need a serializer, like an entity's
/// state, command and event types.</para>
/// </remarks>
public sealed record WorkflowDefinition<TInput, TState, TFact, TOutcome>(
    WorkflowTypeName TypeName,
    Func<TInput, TState> Start,
    Func<TState, WorkContext<TInput, TFact>, WorkList<TFact>> Work,
    Func<TState, TFact, DecideContext<TInput, TState, TOutcome>, Transition<TState, TOutcome>> Decide,
    Func<TOutcome, OutcomeKind> Kind,
    Func<CancelRequest, TFact> Cancel,
    Func<WorkGaveUp, TFact> GaveUp,
    WorkflowOptions? Options = null)
    where TInput : notnull
    where TState : notnull
    where TFact : notnull
    where TOutcome : notnull
{
    internal ImmutableDictionary<Type, Func<object, TFact>> Accepted { get; init; } =
        ImmutableDictionary<Type, Func<object, TFact>>.Empty;

    /// <summary>
    /// Declares how a message of another type becomes one of this workflow's facts. An existing actor can
    /// then send its own message type to the workflow's address. It gets no reply.
    /// </summary>
    public WorkflowDefinition<TInput, TState, TFact, TOutcome> Accepts<TMessage>(Func<TMessage, TFact> wrap)
        where TMessage : notnull
    {
        ArgumentNullException.ThrowIfNull(wrap);
        return this with { Accepted = Accepted.SetItem(typeof(TMessage), message => wrap((TMessage)message)) };
    }
}

/// <summary>
/// One thing a state needs: work (<see cref="ExecuteWork{TFact}"/>) or a wait for a point in time
/// (<see cref="TimerWork{TFact}"/>). Build it with <see cref="WorkContext{TInput,TFact}"/>.
/// </summary>
/// <remarks>
/// <para>Work and a wait are different. Work is a call to a service. It holds facts from outside while it has
/// no answer, and it gives up after <see cref="WorkflowOptions.AttemptLimit"/>.</para>
/// <para>A wait holds nothing: a fact from outside is decided at once. It can last months and it never gives
/// up. When it is due, its fact is decided, unless the workflow has left the state by then.</para>
/// </remarks>
public abstract record Work<TFact>
{
    private protected Work()
    {
    }

    // Only the library derives from Work: a type outside it cannot override an internal abstract member.
    internal abstract bool IsWait { get; }

    public static implicit operator WorkList<TFact>(Work<TFact> work) => new([work]);
}

/// <summary>
/// A call to the <see cref="IEffectHandler{TEffect,TOutcome}"/> registered for the effect's type, whose result
/// is one of the workflow's facts.
/// </summary>
/// <remarks>The effect is plain data: assert on it, log it. Two pieces of work are equal when their effects are.</remarks>
public sealed record ExecuteWork<TFact> : Work<TFact>
{
    internal ExecuteWork(object effect) => Effect = effect;

    /// <summary>The effect the handler receives.</summary>
    public object Effect { get; }

    internal override bool IsWait => false;

    // Bound when the work is built, where TEffect is known, so no reflection is needed to find the handler.
    internal Func<IServiceProvider, Func<CancellationToken, Task<TFact>>>? Resolve { get; init; }

    public bool Equals(ExecuteWork<TFact>? other) => other is not null && Equals(Effect, other.Effect);

    public override int GetHashCode() => Effect.GetHashCode();
}

/// <summary>
/// A wait for a point in time. When it is due, the workflow receives <see cref="Fact"/>.
/// </summary>
/// <remarks>
/// <para>The point in time is a duration counted from when the state was entered (<see cref="After"/>), or a
/// date (<see cref="At"/>). Two timers are equal when their rule and their fact are equal.</para>
/// <para>A timer is kept by a durable reminder, so it fires after a restart too. When the workflow leaves the
/// state, the timer is gone. <c>Stay</c> keeps the state, so its timers stay armed.</para>
/// </remarks>
public sealed record TimerWork<TFact> : Work<TFact>
{
    internal TimerWork(TimeSpan? after, DateTimeOffset? at, TFact fact, DateTimeOffset due)
    {
        After = after;
        At = at;
        Fact = fact;
        Due = due;
    }

    /// <summary>The wait, counted from when the state was entered. Null for a timer given as a date.</summary>
    public TimeSpan? After { get; }

    /// <summary>The date the timer is due. Null for a timer given as a duration.</summary>
    public DateTimeOffset? At { get; }

    /// <summary>The fact the workflow receives when the timer is due.</summary>
    public TFact Fact { get; }

    /// <summary>When the timer is due.</summary>
    public DateTimeOffset Due { get; }

    internal override bool IsWait => true;

    public bool Equals(TimerWork<TFact>? other) =>
        other is not null && After == other.After && At == other.At
        && EqualityComparer<TFact>.Default.Equals(Fact, other.Fact);

    public override int GetHashCode() => HashCode.Combine(After, At, Fact);
}

/// <summary>What a state needs. Empty for a state that waits only for facts from outside.</summary>
public sealed record WorkList<TFact>(ImmutableArray<Work<TFact>> Items)
{
    public static readonly WorkList<TFact> Nothing = new([]);

    public bool Equals(WorkList<TFact>? other) => other is not null && Items.SequenceEqual(other.Items);

    public override int GetHashCode() => Items.Length;
}

/// <summary>
/// What <c>Work</c> may read, plus builders for work. It has no clock: a state's work must be the same every
/// time it is asked for. A timer's duration is counted from <see cref="EnteredAt"/>, which is journaled.
/// </summary>
/// <param name="Id">The workflow's id.</param>
/// <param name="Input">The record the workflow started with.</param>
/// <param name="EnteredAt">When the workflow entered the state. Journaled, so it does not change.</param>
public readonly record struct WorkContext<TInput, TFact>(WorkflowId Id, TInput Input, DateTimeOffset EnteredAt)
{
    /// <summary>
    /// A call to the <see cref="IEffectHandler{TEffect,TOutcome}"/> registered for <typeparamref name="TEffect"/>
    /// with the workflow's fact type as its outcome. Build the effect from the state and the input only.
    /// </summary>
    /// <remarks>
    /// Work holds facts from outside while it has no answer. They are decided after the work's own fact.
    /// </remarks>
    public ExecuteWork<TFact> Execute<TEffect>(TEffect effect) where TEffect : notnull
    {
        ArgumentNullException.ThrowIfNull(effect);
        return new ExecuteWork<TFact>(effect) { Resolve = services => WorkHandlers.Resolve<TEffect, TFact>(services, effect) };
    }

    /// <summary>
    /// A wait that ends <paramref name="after"/> the workflow entered the state. Then the workflow receives
    /// <paramref name="fact"/>.
    /// </summary>
    /// <remarks>
    /// A wait is not work. It holds nothing: a fact from outside is decided at once. It never gives up.
    /// <c>Again</c> keeps the time the state was entered, so a duration that has passed is due again at once.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="after"/> is not positive.</exception>
    public TimerWork<TFact> Timer(TimeSpan after, TFact fact)
    {
        if (after <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(after), after, "A timer's duration must be positive.");
        ArgumentNullException.ThrowIfNull(fact);
        return new TimerWork<TFact>(after, at: null, fact, EnteredAt + after);
    }

    /// <summary>A wait that ends at <paramref name="at"/>. Then the workflow receives <paramref name="fact"/>.</summary>
    /// <remarks>
    /// A wait is not work. It holds nothing: a fact from outside is decided at once. It never gives up.
    /// A date that has passed is due at once.
    /// </remarks>
    public TimerWork<TFact> Timer(DateTimeOffset at, TFact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);
        return new TimerWork<TFact>(after: null, at, fact, at);
    }

    /// <summary>Several things in one state: work, timers, or both. Positions follow the order given.</summary>
    public WorkList<TFact> All(params Work<TFact>[] work)
    {
        ArgumentNullException.ThrowIfNull(work);
        foreach (var item in work)
            ArgumentNullException.ThrowIfNull(item, nameof(work));
        return new WorkList<TFact>([.. work]);
    }

    /// <summary>For a state that has no work: it waits for facts from outside.</summary>
    public WorkList<TFact> Nothing() => WorkList<TFact>.Nothing;
}

/// <summary>What <c>Decide</c> may read, plus builders for transitions.</summary>
/// <param name="Id">The workflow's id.</param>
/// <param name="Input">The record the workflow started with.</param>
/// <param name="Now">The current time. A decision is journaled, so it may depend on the time.</param>
/// <param name="EnteredAt">When the workflow entered the current state.</param>
public readonly record struct DecideContext<TInput, TState, TOutcome>(
    WorkflowId Id,
    TInput Input,
    DateTimeOffset Now,
    DateTimeOffset EnteredAt)
{
    /// <summary>Move to a state and issue its work.</summary>
    public Transition<TState, TOutcome> Next(TState state) => new NextTransition<TState>(state);

    /// <summary>End the workflow with an outcome.</summary>
    public Transition<TState, TOutcome> Finish(TOutcome outcome) => new FinishTransition<TOutcome>(outcome);

    /// <summary>Nothing changes. For a duplicate, or a fact that no longer matters.</summary>
    public Transition<TState, TOutcome> Stay() => new StayTransition();

    /// <summary>Keep the state and issue its work again. The deliberate way to repeat work.</summary>
    public Transition<TState, TOutcome> Again() => new AgainTransition();

    /// <summary>
    /// For a pair of state and fact that should not happen. The fact is journaled and a warning is logged;
    /// the state does not change.
    /// </summary>
    public Transition<TState, TOutcome> Unexpected() => new UnexpectedTransition();
}

public sealed record NextTransition<TState>(TState State);
public sealed record FinishTransition<TOutcome>(TOutcome Outcome);
public sealed record StayTransition;
public sealed record AgainTransition;
public sealed record UnexpectedTransition;

/// <summary>What a decision does to the workflow. Build one with <see cref="DecideContext{TInput,TState,TOutcome}"/>.</summary>
public readonly union Transition<TState, TOutcome>(
    NextTransition<TState>,
    FinishTransition<TOutcome>,
    StayTransition,
    AgainTransition,
    UnexpectedTransition);

internal static class WorkHandlers
{
    /// <summary>
    /// Returns a call to the handler registered for <typeparamref name="TEffect"/>. A missing handler is not a
    /// crash: the call fails, and the workflow receives the give-up fact like any other work without an answer.
    /// </summary>
    internal static Func<CancellationToken, Task<TFact>> Resolve<TEffect, TFact>(IServiceProvider services, TEffect effect)
        where TEffect : notnull
    {
        var handler = services.GetService<IEffectHandler<TEffect, TFact>>();
        if (handler is null)
            return _ => Task.FromException<TFact>(new InvalidOperationException(
                $"No effect handler is registered for {typeof(TEffect)}. Register an " +
                $"IEffectHandler<{typeof(TEffect).Name}, {typeof(TFact).Name}>."));

        return ct => handler.HandleAsync(effect, ct);
    }
}
