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

// Built-in, in-process effects. None of these are journaled: an entity that must survive a crash
// with an effect in flight records that fact in its state and re-derives the effect in Resume.
public sealed record ReplyEffect(object Response);
public sealed record TellEffect(ICanTell Recipient, object Message);

/// <summary>
/// A call to the <see cref="IEffectHandler{TEffect,TOutcome}"/> registered for <see cref="Effect"/>'s type.
/// The handler's result comes back to the entity as one of its own commands or events.
/// </summary>
/// <remarks>
/// The effect is plain data: assert on it, log it, store it in state. The handler is resolved from a fresh
/// DI scope when the effect runs, so <c>Decide</c> never sees a service. The entity does not wait for the
/// outcome; it keeps handling commands and the outcome arrives as a message.
/// </remarks>
public sealed record ExecuteEffect(object Effect)
{
    // Bound when the effect is built, where TEffect is known, so no reflection is needed to find the handler.
    internal Func<IServiceProvider, Func<CancellationToken, Task<object?>>>? Resolve { get; init; }

    // Turns a handler failure or timeout into an outcome. Without it the failure is logged and dropped.
    internal Func<Exception, object?>? Failed { get; init; }

    public bool Equals(ExecuteEffect? other) => other is not null && Equals(Effect, other.Effect);

    public override int GetHashCode() => Effect.GetHashCode();
}

/// <summary>
/// A command for another durable entity, addressed by its command protocol and <see cref="EntityId"/>.
/// </summary>
/// <remarks>
/// The target's reply, if it sends one, comes back to this entity and is turned into one of its own
/// commands by <see cref="EntityDefinition{TState,TCommand,TEvent}.Accepts{TMessage}"/>. The reply is
/// handled with the same sender as the command that issued the send, so a later <c>Reply</c> reaches the
/// original asker.
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
public sealed record CancelTimerEffect(string Key);

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
    /// Runs <paramref name="effect"/> through its registered <see cref="IEffectHandler{TEffect,TOutcome}"/>.
    /// The handler's result returns to the entity as a command, and <c>Decide</c> decides what it means.
    /// </summary>
    public Handled<TCommand, TEvent> Execute<TEffect>(TEffect effect) where TEffect : notnull
    {
        ArgumentNullException.ThrowIfNull(effect);
        return Add(new ExecuteEffect(effect)
        {
            Resolve = services => EffectHandlers.Resolve<TEffect, TCommand>(services, effect),
        });
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

internal static class EffectHandlers
{
    /// <summary>
    /// Finds the one handler registered for <typeparamref name="TEffect"/> and returns a call to it. The
    /// handler returns one of the entity's commands, which <c>Decide</c> then decides on.
    /// </summary>
    internal static Func<CancellationToken, Task<object?>> Resolve<TEffect, TCommand>(
        IServiceProvider services, TEffect effect) where TEffect : notnull
    {
        var handler = services.GetService<IEffectHandler<TEffect, TCommand>>()
            ?? throw new InvalidOperationException(
                $"No effect handler is registered for {typeof(TEffect)}. Register an " +
                $"IEffectHandler<{typeof(TEffect).Name}, {typeof(TCommand).Name}>.");

        return async ct => await handler.HandleAsync(effect, ct);
    }
}
