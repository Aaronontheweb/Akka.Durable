using System.Collections.Immutable;
using Akka.Actor;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

// The command-processing outcomes are mutually exclusive.
// Only Handled<TCommand, TEvent> can contain events and effects.
public sealed record Ignored;
public sealed record NotHandled;
public sealed record Stashed;

// Built-in, in-process effects. Only an Execute that a decision issues is journaled. The others are not: an entity
// that must not lose one keeps that fact in its state.
public sealed record ReplyEffect(object Response);
public sealed record TellEffect(ICanTell Recipient, object Message);

/// <summary>
/// A call to the effect handler registered for <see cref="Effect"/>'s type. The handler's answer, or the effect's
/// give-up, comes back to the entity as one of its own commands.
/// </summary>
/// <remarks>
/// <para>The effect is plain data: assert on it, log it. The handler is resolved from a fresh DI scope when the effect
/// runs, so <c>Decide</c> never sees a service. The entity does not wait for the answer; it keeps handling commands and
/// the answer arrives as a message.</para>
/// <para>An effect a decision issues with <c>Execute</c> is journaled in the same write as the decision's events.</para>
/// </remarks>
public sealed record ExecuteEffect(object Effect)
{
    // Bound when the effect is built, where TEffect is known, so no reflection is needed to find the handler.
    // Null for a recorded effect: its handler is found by its route, also after a restart.
    internal Func<IServiceProvider, Func<CancellationToken, Task<object?>>>? Resolve { get; init; }

    // Turns a handler failure or timeout into an outcome. Without it the failure is logged and dropped.
    internal Func<Exception, object?>? Failed { get; init; }

    /// <summary>
    /// Journaled with the decision that issues it, attempted once, and answered or given up. True for the effect of
    /// <c>Handled.Execute</c>. The library's own calls, and a workflow's work, are not recorded this way.
    /// </summary>
    internal bool Recorded { get; init; }

    public bool Equals(ExecuteEffect? other) => other is not null && Equals(Effect, other.Effect);

    public override int GetHashCode() => Effect.GetHashCode();
}

/// <summary>
/// A command for another durable entity, addressed by its command protocol and <see cref="EntityId"/>.
/// </summary>
/// <remarks>
/// <para>From an entity: the target's reply, if it sends one, comes back to this entity and is turned into one
/// of its own commands by <see cref="EntityDefinition{TState,TCommand,TEvent}.Accepts{TMessage}"/>. The reply is
/// handled with the same sender as the command that issued the send, so a later <c>Reply</c> reaches the
/// original asker.</para>
/// <para>From a workflow's <c>Work</c>: it is the effect of the work that <c>ctx.Send</c> builds, and the reply
/// becomes one of the workflow's facts.</para>
/// </remarks>
public sealed record SendEffect(Type CommandType, EntityId Target, object Command)
{
    internal Func<ActorSystem, IActorRef>? ResolveRegion { get; init; }

    public bool Equals(SendEffect? other) =>
        other is not null && CommandType == other.CommandType && Target.Equals(other.Target) && Equals(Command, other.Command);

    public override int GetHashCode() => HashCode.Combine(CommandType, Target, Command);
}

// Scheduling effects are backed by Akka.Reminders: the reminder is persisted by the
// reminder subsystem and delivered back to this entity at least once. Registration
// happens after the decision's events commit and is NOT atomic with the journal write.
public sealed record ScheduleEffect(string Key, TimeSpan Delay, object Message);
public sealed record ScheduleRecurringEffect(string Key, TimeSpan InitialDelay, TimeSpan Interval, object Message);
// BestEffort: a cancel the reminder store refuses is logged and the entity carries on, instead of failing.
public sealed record CancelTimerEffect(string Key, bool BestEffort = false);

public sealed record UnstashAllEffect;
public sealed record StopEffect;

/// <summary>Best-effort asynchronous work whose result the entity does not need.</summary>
/// <remarks>
/// Runs in-process in a fresh DI scope and is awaited in place: later effects of the same decision, and
/// later commands, wait for it. A failure or timeout is logged and the decision continues. Nothing about it
/// is journaled. When the result matters to the entity, use <c>Execute</c> instead.
/// </remarks>
public sealed class RunEffect
{
    internal RunEffect(Func<IServiceProvider, CancellationToken, Task> executeAsync, TimeSpan? timeout)
    {
        ExecuteAsync = executeAsync;
        Timeout = timeout;
    }

    public Func<IServiceProvider, CancellationToken, Task> ExecuteAsync { get; }
    public TimeSpan? Timeout { get; }
}

public readonly union EntityEffect(
    ReplyEffect,
    TellEffect,
    ExecuteEffect,
    SendEffect,
    RunEffect,
    ScheduleEffect,
    ScheduleRecurringEffect,
    CancelTimerEffect,
    UnstashAllEffect,
    StopEffect);

/// <summary>
/// A finished decision: events to persist, then effects to run in order after the commit.
/// </summary>
/// <remarks>
/// Every fluent method returns a new immutable instance. Scheduled messages are typed as
/// <typeparamref name="TCommand"/>, so the entity can always decide on them. For C# 15 union commands,
/// scheduling also forces the union conversion at the call site rather than boxing the bare case type.
/// </remarks>
public sealed record Handled<TCommand, TEvent>(
    ImmutableArray<TEvent> Events,
    ImmutableArray<EntityEffect> Effects)
{
    internal Handled<TCommand, TEvent> Add(EntityEffect effect)
    {
        // Stop ends the incarnation, so nothing can run after it.
        if (!Effects.IsDefaultOrEmpty && Effects[^1] is StopEffect)
            throw new InvalidOperationException("Stop() must be the last effect in a decision.");

        return this with { Effects = Effects.IsDefault ? [effect] : Effects.Add(effect) };
    }

    /// <summary>Appends events to persist before any effect runs.</summary>
    public Handled<TCommand, TEvent> Persist(params TEvent[] events) =>
        this with { Events = Events.IsDefault ? [.. events] : Events.AddRange(events) };

    /// <summary>
    /// Replies to the sender of the command being handled. <paramref name="request"/> fixes the reply type
    /// and is not sent anywhere: pass the command that declared the reply, even when it arrived earlier and
    /// was kept in state.
    /// </summary>
    public Handled<TCommand, TEvent> Reply<TReply>(IReplies<TCommand, TReply> request, TReply reply)
        where TReply : notnull
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(reply);
        return Add(new ReplyEffect(reply));
    }

    /// <summary>
    /// Issues <paramref name="effect"/> to the handler registered for its type with <c>AddEffectHandler</c>. The
    /// effect is journaled in the same write as this decision's events, and attempted once after the write.
    /// </summary>
    /// <remarks>
    /// <para><c>Decide</c> then gets exactly one of two things, as a command: the handler's answer, or the effect's
    /// give-up (<see cref="IEffect{TCommand}.GaveUp"/>) with <see cref="GaveUpReason.Failed"/>,
    /// <see cref="GaveUpReason.TimedOut"/> or, after a crash or a restart, <see cref="GaveUpReason.Interrupted"/>. A
    /// handler with no answer (<see cref="IEffectHandler{TEffect}"/>) is acknowledged when it returns, and
    /// <c>Decide</c> gets nothing. The library never runs the effect again.</para>
    /// <para>The effect type needs a serializer and a handler registered with <c>AddEffectHandler</c>. Without a
    /// handler the effect is refused before the write, and the entity restarts.</para>
    /// </remarks>
    public Handled<TCommand, TEvent> Execute<TEffect>(TEffect effect) where TEffect : notnull, IEffect<TCommand>
    {
        ArgumentNullException.ThrowIfNull(effect);
        return Add(new ExecuteEffect(effect) { Recorded = true });
    }

    /// <summary>
    /// Sends a command to another durable entity hosted in this actor system. Its reply comes back as one of
    /// this entity's commands through <see cref="EntityDefinition{TState,TCommand,TEvent}.Accepts{TMessage}"/>.
    /// </summary>
    /// <remarks>
    /// Name the target's command protocol explicitly, as in <c>Send&lt;InvoiceCommand&gt;(id, new IssueInvoice(...))</c>:
    /// the compiler would otherwise infer the case type, which no entity type is registered under.
    /// </remarks>
    public Handled<TCommand, TEvent> Send<TOther>(EntityId target, TOther command)
    {
        ArgumentNullException.ThrowIfNull(command);
        return Add(new SendEffect(typeof(TOther), target, command)
        {
            ResolveRegion = system => ActorRegistry.For(system).Get<EntityRegion<TOther>>(),
        });
    }

    /// <summary>
    /// Starts the workflow <paramref name="id"/> of the workflow type <paramref name="workflow"/> with
    /// <paramref name="input"/>, after this decision's events are journaled. The library completes the start.
    /// </summary>
    /// <remarks>
    /// <para>The entity's definition must name the workflow type with <c>Uses</c>.</para>
    /// <para>Until the workflow answers, a durable reminder keeps the start. If the start gets no answer, because
    /// the call failed, timed out or the process stopped, the reminder issues it again after
    /// <see cref="EntityOptions.EffectTimeout"/>, also after a restart. The repeats stop when the workflow answers
    /// <see cref="Started"/>, <see cref="AlreadyRunning"/> or <see cref="AlreadyFinished{TOutcome}"/>. A start is
    /// addressed by id, so a repeat is safe. The answer is not delivered to the entity: use the overload with
    /// <c>then</c> for that.</para>
    /// </remarks>
    public Handled<TCommand, TEvent> Start<TInput, TState, TFact, TOutcome>(
        WorkflowDefinition<TInput, TState, TFact, TOutcome> workflow, WorkflowId id, TInput input)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);
        ArgumentNullException.ThrowIfNull(input);
        return Add(WorkflowCalls.Start<TInput, TState, TFact, TOutcome>(workflow.TypeName, id, input, then: null, gaveUp: null));
    }

    /// <summary>
    /// Starts the workflow <paramref name="id"/> like the overload without <c>then</c>, and delivers the answer as one
    /// of this entity's commands.
    /// </summary>
    /// <param name="workflow">The workflow type's definition. The entity's definition must name it with <c>Uses</c>.</param>
    /// <param name="id">The workflow's id. Derive it from this entity's id.</param>
    /// <param name="input">The record the workflow starts with.</param>
    /// <param name="then">Turns the workflow's answer into one of this entity's commands.</param>
    /// <param name="gaveUp">
    /// Turns a start that got no answer, because it failed or ran past <see cref="EntityOptions.EffectTimeout"/>, into
    /// one of this entity's commands. The library then stops repeating the start: the entity decides, and it may start
    /// again.
    /// </param>
    /// <remarks>
    /// After a restart the library still issues a start that was not answered, but the functions belong to the
    /// incarnation that issued it, so the answer is not delivered. An entity that needs the answer after a restart
    /// keeps "waiting for the start" in its state and issues this start again from <c>Resume</c>.
    /// </remarks>
    public Handled<TCommand, TEvent> Start<TInput, TState, TFact, TOutcome>(
        WorkflowDefinition<TInput, TState, TFact, TOutcome> workflow, WorkflowId id, TInput input,
        Func<StartResult<TOutcome>, TCommand> then, Func<WorkGaveUp, TCommand> gaveUp)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(then);
        ArgumentNullException.ThrowIfNull(gaveUp);
        return Add(WorkflowCalls.Start<TInput, TState, TFact, TOutcome>(
            workflow.TypeName, id, input, answer => then(answer), failure => gaveUp(failure)));
    }

    /// <summary>
    /// Sends <paramref name="fact"/> to the workflow <paramref name="id"/>, after this decision's events are journaled.
    /// The answer, or the give-up, becomes one of this entity's commands.
    /// </summary>
    /// <param name="workflow">The workflow type's definition. The entity's definition must name it with <c>Uses</c>.</param>
    /// <param name="id">The workflow's id.</param>
    /// <param name="fact">The fact. The compiler checks it is of the workflow's fact type.</param>
    /// <param name="then">
    /// Turns the answer into one of this entity's commands: <see cref="Delivered"/>, <see cref="Held"/>,
    /// <see cref="WorkflowNotStarted"/> or <see cref="AlreadyFinished{TOutcome}"/>.
    /// </param>
    /// <param name="gaveUp">
    /// Turns a call that got no answer, because it failed or ran past <see cref="EntityOptions.EffectTimeout"/>, into one
    /// of this entity's commands. The fact may have been journaled.
    /// </param>
    /// <remarks>
    /// The fact is sent once. The library does not repeat it, also not after a restart: a repeated fact would be
    /// journaled twice. To send it again, the entity decides so.
    /// </remarks>
    public Handled<TCommand, TEvent> Notify<TInput, TState, TFact, TOutcome>(
        WorkflowDefinition<TInput, TState, TFact, TOutcome> workflow, WorkflowId id, TFact fact,
        Func<DeliveryResult<TOutcome>, TCommand> then, Func<WorkGaveUp, TCommand> gaveUp)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);
        ArgumentNullException.ThrowIfNull(fact);
        ArgumentNullException.ThrowIfNull(then);
        ArgumentNullException.ThrowIfNull(gaveUp);
        return Add(WorkflowCalls.Notify<TInput, TState, TFact, TOutcome>(
            workflow.TypeName, id, fact, answer => then(answer), failure => gaveUp(failure)));
    }

    /// <summary>
    /// Asks the workflow <paramref name="id"/> to stop, after this decision's events are journaled. The answer, or the
    /// give-up, becomes one of this entity's commands.
    /// </summary>
    /// <param name="workflow">The workflow type's definition. The entity's definition must name it with <c>Uses</c>.</param>
    /// <param name="id">The workflow's id.</param>
    /// <param name="reason">Why the workflow should stop. Required, and not empty.</param>
    /// <param name="by">
    /// Who or what asked. Required, and not empty. Use an id, not a person's name or e-mail address: the value goes
    /// into the journal and the log.
    /// </param>
    /// <param name="then">Turns the answer into one of this entity's commands.</param>
    /// <param name="gaveUp">Turns a call that got no answer into one of this entity's commands.</param>
    /// <remarks>Sent once, like <c>Notify</c>. The workflow's table decides what the cancel means.</remarks>
    /// <exception cref="ArgumentException"><paramref name="reason"/> or <paramref name="by"/> is null or empty.</exception>
    public Handled<TCommand, TEvent> Cancel<TInput, TState, TFact, TOutcome>(
        WorkflowDefinition<TInput, TState, TFact, TOutcome> workflow, WorkflowId id, string reason, string by,
        Func<DeliveryResult<TOutcome>, TCommand> then, Func<WorkGaveUp, TCommand> gaveUp)
        where TInput : notnull
        where TState : notnull
        where TFact : notnull
        where TOutcome : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);
        ArgumentException.ThrowIfNullOrEmpty(reason);
        ArgumentException.ThrowIfNullOrEmpty(by);
        ArgumentNullException.ThrowIfNull(then);
        ArgumentNullException.ThrowIfNull(gaveUp);
        return Add(WorkflowCalls.Cancel<TInput, TState, TFact, TOutcome>(
            workflow.TypeName, id, reason, by, answer => then(answer), failure => gaveUp(failure)));
    }

    public Handled<TCommand, TEvent> Tell<TMessage>(
        ICanTell recipient,
        TMessage message)
        where TMessage : notnull =>
        Add(new TellEffect(recipient, message));

    /// <summary>Best-effort work with no outcome: a projection write, a notification. See <see cref="RunEffect"/>.</summary>
    public Handled<TCommand, TEvent> Run<TService>(
        Func<TService, CancellationToken, Task> action,
        TimeSpan? timeout = null) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(action);
        if (timeout is { } limit && limit <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "An effect timeout must be positive.");

        return Add(new RunEffect((services, ct) => action(services.GetRequiredService<TService>(), ct), timeout));
    }

    // Durable reminders via Akka.Reminders, scoped to this entity. Delivery is
    // at-least-once, so deciders must tolerate duplicate occurrences. Scheduling
    // with an existing key overwrites that reminder.
    public Handled<TCommand, TEvent> Schedule(
        string key,
        TimeSpan delay,
        TCommand message) =>
        Add(new ScheduleEffect(key, delay, message!));

    public Handled<TCommand, TEvent> Schedule(string key,
        TimeSpan initialDelay,
        TimeSpan interval,
        TCommand message) =>
        Add(new ScheduleRecurringEffect(key, initialDelay, interval, message!));

    public Handled<TCommand, TEvent> CancelTimer(string key) =>
        Add(new CancelTimerEffect(key));

    public Handled<TCommand, TEvent> UnstashAll() =>
        Add(new UnstashAllEffect());

    // Stops the current actor incarnation after preceding effects. It does not
    // delete the entity's journal, snapshots, or logical identity.
    public Handled<TCommand, TEvent> Stop() =>
        Add(new StopEffect());

    /// <summary>The union conversion as a method, for F# and other callers without implicit conversions.</summary>
    public Decision<TCommand, TEvent> ToDecision() => this;
}

public readonly union Decision<TCommand, TEvent>(Handled<TCommand, TEvent>, Ignored, NotHandled, Stashed);

/// <summary>
/// Explicitly typed decision factories. C# deciders normally use the equivalent methods on
/// <see cref="DecisionContext{TState,TCommand,TEvent}"/>; these exist for F# and other callers.
/// </summary>
public static class Decisions
{
    public static Handled<TCommand, TEvent> Persist<TCommand, TEvent>(params TEvent[] events) =>
        new([.. events], []);

    public static Handled<TCommand, TEvent> None<TCommand, TEvent>() => new([], []);

    public static Ignored Ignore() => new();
    public static NotHandled Unhandled() => new();
    public static Stashed Stash() => new();
}

/// <summary>
/// No effect handler is registered for an effect. A deployment mistake, not a failure of the handler, so the logging
/// site tells it apart by type and says it loudly.
/// </summary>
/// <param name="effectType">The effect's type.</param>
/// <param name="outcomeType">The answer the caller expects: the entity's command type, or the workflow's fact type.</param>
internal sealed class MissingEffectHandlerException(Type effectType, Type outcomeType) : InvalidOperationException(
    $"No effect handler is registered for {effectType.Name}. Register one at startup with " +
    $"services.{Registration(effectType, outcomeType)}.")
{
    internal string EffectType { get; } = effectType.Name;
    internal string HandlerInterface { get; } = Registration(effectType, outcomeType);

    private static string Registration(Type effect, Type outcome) =>
        $"AddEffectHandler<{effect.Name}, {outcome.Name}, YourHandler>()";
}
