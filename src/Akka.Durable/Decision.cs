using System.Collections.Immutable;
using Akka.Actor;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

// The command-processing outcomes are mutually exclusive.
// Only Handled<TEvent> can contain events and effects.
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

// Executed only after the decision's events have been persisted successfully,
// sequentially: no further commands are processed until it completes.
// The interpreter supplies an IServiceProvider scoped to this execution attempt.
// Returning null means there is no completion message to send to the entity.
// Returning a message means the interpreter sends it back through the mailbox.
// If execution fails or times out, the interpreter maps the error through OnFailure
// when set; otherwise the failure propagates to actor supervision. Cancellation
// during actor shutdown is not treated as a domain failure.
public sealed record RunEffect(
    Func<IServiceProvider, CancellationToken, Task<object?>> ExecuteAsync,
    Func<Exception, object>? OnFailure = null,
    TimeSpan? Timeout = null);

public readonly union EntityEffect(
    ReplyEffect,
    TellEffect,
    ScheduleEffect,
    ScheduleRecurringEffect,
    CancelTimerEffect,
    UnstashAllEffect,
    StopEffect,
    RunEffect);

/// <summary>
/// A finished decision: events to persist, then effects to run in order after the commit.
/// </summary>
/// <remarks>
/// Every fluent method returns a new immutable instance. Effects that feed a message back
/// into this entity (reminders, Run completions) are typed as <typeparamref name="TCommand"/>,
/// so the entity can always decide on them. For C# 15 union commands, this also forces the
/// union conversion at the call site rather than boxing the bare case type.
/// </remarks>
public sealed record Handled<TCommand, TEvent>(
    ImmutableArray<TEvent> Events,
    ImmutableArray<EntityEffect> Effects)
{
    private Handled<TCommand, TEvent> Add(EntityEffect effect)
    {
        // Stop ends the current incarnation, so nothing may be sequenced after it.
        if (!Effects.IsDefaultOrEmpty && Effects[^1] is StopEffect)
            throw new InvalidOperationException(
                "Stop() must be the last effect in a decision; no effects may be added after it.");

        return this with { Effects = Effects.IsDefault ? [effect] : Effects.Add(effect) };
    }

    public Handled<TCommand, TEvent> Reply<TReply>(TReply response)
        where TReply : notnull =>
        Add(new ReplyEffect(response));

    public Handled<TCommand, TEvent> Tell<TMessage>(
        ICanTell recipient,
        TMessage message)
        where TMessage : notnull =>
        Add(new TellEffect(recipient, message));

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

    // Best-effort async effect, with no response expected.
    // The service is resolved by the framework, not by the decider.
    public Handled<TCommand, TEvent> Run<TService>(
        Func<TService, CancellationToken, Task> action,
        Func<Exception, TCommand>? onFailure = null,
        TimeSpan? timeout = null)
        where TService : notnull =>
        Add(new RunEffect(
            async (services, ct) =>
            {
                var service = services.GetRequiredService<TService>();
                await action(service, ct);
                return null;
            },
            Box(onFailure), timeout));

    // Best-effort async effect whose result becomes a normal entity message.
    // The framework interprets the effect and sends that message through the
    // entity mailbox; the decider processes it like any other command.
    public Handled<TCommand, TEvent> Run<TService, TResult>(
        Func<TService, CancellationToken, Task<TResult>> action,
        Func<TResult, TCommand> onSuccess,
        Func<Exception, TCommand> onFailure,
        TimeSpan? timeout = null)
        where TService : notnull =>
        Add(new RunEffect(
            async (services, ct) =>
            {
                var service = services.GetRequiredService<TService>();
                var result = await action(service, ct);
                return onSuccess(result);
            },
            Box(onFailure), timeout));

    private static Func<Exception, object>? Box(Func<Exception, TCommand>? onFailure) =>
        onFailure is null ? null : ex => onFailure(ex)!;
}

public readonly union Decision<TCommand, TEvent>(Handled<TCommand, TEvent>, Ignored, NotHandled, Stashed);

/// <summary>
/// Explicitly typed decision factories. C# deciders normally use the equivalent methods on
/// <see cref="DecisionContext{TCommand,TEvent}"/>; these exist for F# and other callers.
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
