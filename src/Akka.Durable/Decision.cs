using System.Collections.Immutable;
using Akka.Actor;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

// The command-processing outcomes are mutually exclusive.
// Only Handled<TCommand, TEvent> can contain events and effects.
public sealed record Ignored;
public sealed record NotHandled;
public sealed record Stashed;

// Built-in, in-process effects. None of these are journaled.
// In particular, RunEffect is a deferred delegate, NOT a serialized operation.
public sealed record ReplyEffect(object Response);
public sealed record TellEffect(ICanTell Recipient, object Message);

// Scheduling effects are backed by Akka.Reminders: the reminder is persisted by the
// reminder subsystem and delivered back to this entity at least once. Registration
// happens after the decision's events commit and is NOT atomic with the journal write.
public sealed record ScheduleEffect(string Key, TimeSpan Delay, object Message);
public sealed record ScheduleRecurringEffect(string Key, TimeSpan InitialDelay, TimeSpan Interval, object Message);
public sealed record CancelTimerEffect(string Key);

public sealed record UnstashAllEffect;
public sealed record StopEffect;

/// <summary>An asynchronous operation with explicit success and failure continuations.</summary>
/// <remarks>Runs in-process; neither the delegate nor its continuations are journaled.</remarks>
public sealed class RunEffect<TCommand, TEvent>
{
    internal RunEffect(
        Func<IServiceProvider, CancellationToken, Task<object?>> executeAsync,
        Func<object, DateTimeOffset, object?, Handled<TCommand, TEvent>> onSuccess,
        Func<object, DateTimeOffset, Exception, Handled<TCommand, TEvent>> onFailure,
        TimeSpan? timeout)
    {
        ExecuteAsync = executeAsync;
        OnSuccess = onSuccess;
        OnFailure = onFailure;
        Timeout = timeout;
    }

    public Func<IServiceProvider, CancellationToken, Task<object?>> ExecuteAsync { get; }
    public TimeSpan? Timeout { get; }
    internal Func<object, DateTimeOffset, object?, Handled<TCommand, TEvent>> OnSuccess { get; }
    internal Func<object, DateTimeOffset, Exception, Handled<TCommand, TEvent>> OnFailure { get; }

    internal static RunEffect<TCommand, TEvent> Create<TService, TResult>(
        Func<TService, CancellationToken, Task<TResult>> action,
        Func<object, DateTimeOffset, object?, Handled<TCommand, TEvent>> onSuccess,
        Func<object, DateTimeOffset, Exception, Handled<TCommand, TEvent>> onFailure,
        TimeSpan? timeout) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(action);
        if (timeout is { } limit && limit <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "An effect timeout must be positive.");

        return new(
            async (services, ct) => await action(services.GetRequiredService<TService>(), ct),
            onSuccess, onFailure, timeout);
    }
}

public readonly union EntityEffect<TCommand, TEvent>(
    ReplyEffect,
    TellEffect,
    ScheduleEffect,
    ScheduleRecurringEffect,
    CancelTimerEffect,
    UnstashAllEffect,
    StopEffect,
    RunEffect<TCommand, TEvent>);

/// <summary>
/// A finished decision: events to persist, then effects to run in order after the commit.
/// </summary>
/// <remarks>
/// Every fluent method returns a new immutable instance. Scheduled messages are typed as
/// <typeparamref name="TCommand"/>, so the entity can always decide on them.
/// Run continuations return decisions. For C# 15 union commands, scheduling also forces the
/// union conversion at the call site rather than boxing the bare case type.
/// </remarks>
public sealed record Handled<TCommand, TEvent>(
    ImmutableArray<TEvent> Events,
    ImmutableArray<EntityEffect<TCommand, TEvent>> Effects)
{
    internal Handled<TCommand, TEvent> Add(EntityEffect<TCommand, TEvent> effect)
    {
        // Stop ends the incarnation; Run transfers control to exactly one continuation.
        if (!Effects.IsDefaultOrEmpty && Effects[^1] is StopEffect or RunEffect<TCommand, TEvent>)
            throw new InvalidOperationException(
                "Stop() and Run() must be the last effect in a decision; place subsequent work in a Run continuation.");

        return this with { Effects = Effects.IsDefault ? [effect] : Effects.Add(effect) };
    }

    public Handled<TCommand, TEvent> Tell<TMessage>(
        ICanTell recipient,
        TMessage message)
        where TMessage : notnull =>
        Add(new TellEffect(recipient, message));

    public Handled<TCommand, TEvent> Tell<TOther>(IEntityRef<TOther> recipient, TOther message)
    {
        ArgumentNullException.ThrowIfNull(recipient);
        return Add(new TellEffect(new EntityTellAdapter<TOther>(recipient), message!));
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

    /// <summary>Branches on an operation's outcome. Both continuations are required.</summary>
    public Handled<TCommand, TEvent> Run<TService, TResult>(
        Func<TService, CancellationToken, Task<TResult>> action,
        Func<TResult, Handled<TCommand, TEvent>> onSuccess,
        Func<Exception, Handled<TCommand, TEvent>> onFailure,
        TimeSpan? timeout = null) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(onSuccess);
        ArgumentNullException.ThrowIfNull(onFailure);
        return Add(RunEffect<TCommand, TEvent>.Create(action,
            (_, _, result) => onSuccess((TResult)result!),
            (_, _, error) => onFailure(error), timeout));
    }

    public Handled<TCommand, TEvent> Run<TService>(
        Func<TService, CancellationToken, Task> action,
        Func<Handled<TCommand, TEvent>> onSuccess,
        Func<Exception, Handled<TCommand, TEvent>> onFailure,
        TimeSpan? timeout = null) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(action);
        ArgumentNullException.ThrowIfNull(onSuccess);
        return Run<TService, bool>(async (service, ct) =>
        {
            await action(service, ct);
            return true;
        }, _ => onSuccess(), onFailure, timeout);
    }
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

internal sealed record EntityTellAdapter<TCommand>(IEntityRef<TCommand> Recipient) : ICanTell
{
    public void Tell(object message, IActorRef sender) => Recipient.Tell((TCommand)message, sender);
}
