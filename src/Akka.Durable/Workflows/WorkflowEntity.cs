using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Akka.Serialization.V2;
using Microsoft.Extensions.Logging;

namespace Akka.Durable;

// A workflow is hosted as an ordinary durable entity. These are that entity's state, events, commands and
// replies. They belong to the library and are not generic: every value of an author's type is an object
// field, which the serializer writes with whatever serializer is bound to the value's type. The author
// therefore registers a serializer for their own types and nothing else.
//
// Everything here is persisted or sent between nodes. Change it extend-only: add fields with new indexes,
// add cases with new manifests, and never reuse or repurpose one.

// ---- State ----

[AkkaSerializable(Manifest = "durable-wf-not-started-v1", AllowEmpty = true)]
internal sealed record WfNotStarted : IDurableEntityProtocol;

/// <param name="Input">The author's input.</param>
/// <param name="State">The author's state.</param>
/// <param name="Entry">
/// Counts the times the workflow has entered a state, including a repeat of the same one. An answer names
/// the entry its work was issued for, so an answer to earlier work is not mistaken for current work.
/// </param>
/// <param name="EnteredAt">When the workflow entered <paramref name="State"/>.</param>
/// <param name="Settled">
/// Positions in the state's work list that have been answered during this entry. A timer is settled when its
/// fact is journaled.
/// </param>
/// <param name="Held">
/// Facts that arrived while work was in flight, in arrival order. They are decided when the work settles.
/// </param>
/// <param name="IssuedAt">
/// When the work of this entry was issued: the time of the start, the move or the repeat.
/// </param>
/// <param name="Attempts">
/// One <see cref="WfAttempt"/> for each position whose attempts are counted: work with a retry policy that failed or
/// started again, and work whose first attempt waited for a delayed <c>Again</c>. A position without one is at attempt
/// 1, started at <paramref name="IssuedAt"/>, unless <paramref name="Delayed"/> is set. Empty in a new entry.
/// </param>
/// <param name="Delayed">
/// Set by <c>Again(after:)</c>: the work of this entry is first issued at this time. Until then nothing is in flight.
/// </param>
[AkkaSerializable(Manifest = "durable-wf-running-v1")]
internal sealed record WfRunning(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object State,
    [property: AkkaField(2)] int Entry,
    [property: AkkaField(3)] DateTimeOffset EnteredAt,
    [property: AkkaField(4)] ImmutableArray<int> Settled,
    [property: AkkaField(5)] ImmutableArray<object> Held,
    [property: AkkaField(6)] DateTimeOffset IssuedAt = default,
    [property: AkkaField(7)] ImmutableArray<object> Attempts = default,
    [property: AkkaField(8)] DateTimeOffset? Delayed = null) : IDurableEntityProtocol
{
    // ImmutableArray compares by reference; compare its contents so records with equal data are equal.
    public bool Equals(WfRunning? other) =>
        other is not null && Equals(Input, other.Input) && Equals(State, other.State) && Entry == other.Entry
        && EnteredAt == other.EnteredAt && Settled.SequenceEqual(other.Settled)
        && Held.SequenceEqual(other.Held) && IssuedAt == other.IssuedAt
        && WorkAttempts.Of(Attempts).SequenceEqual(WorkAttempts.Of(other.Attempts)) && Delayed == other.Delayed;

    public override int GetHashCode() =>
        HashCode.Combine(Input, State, Entry, EnteredAt, Settled.Length, Held.Length, IssuedAt, Delayed);
}

/// <summary>Where one piece of work stands in its attempts.</summary>
/// <param name="Index">The work's position in the state's work list.</param>
/// <param name="Attempt">The number of the last attempt that started or failed.</param>
/// <param name="StartedAt">When that attempt started.</param>
/// <param name="NextAt">
/// Null: the attempt is in flight. Otherwise the attempt failed, and attempt <paramref name="Attempt"/> + 1 is due at
/// this time. Nothing is in flight until then.
/// </param>
[AkkaSerializable(Manifest = "durable-wf-attempt-v1")]
internal sealed record WfAttempt(
    [property: AkkaField(0)] int Index,
    [property: AkkaField(1)] int Attempt,
    [property: AkkaField(2)] DateTimeOffset StartedAt,
    [property: AkkaField(3)] DateTimeOffset? NextAt) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-finished-v1")]
internal sealed record WfFinished(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object Outcome,
    [property: AkkaField(2)] DateTimeOffset At) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-state-v1")]
internal readonly union WfState(WfNotStarted, WfRunning, WfFinished) : IDurableEntityProtocol;

// ---- Events: the start, then each fact with what it led to ----

[AkkaSerializable(Manifest = "durable-wf-started-v1")]
internal sealed record WfStarted(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object First,
    [property: AkkaField(2)] DateTimeOffset At) : IDurableEntityProtocol, IDurableJournalRecord;

[AkkaSerializable(Manifest = "durable-wf-moved-v1")]
internal sealed record WfMoved([property: AkkaField(0)] object Next) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-ended-v1")]
internal sealed record WfEnded([property: AkkaField(0)] object Outcome) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-kept-v1", AllowEmpty = true)]
internal sealed record WfKept : IDurableEntityProtocol;

/// <param name="Until">
/// Null: the state's work is issued again at once. Otherwise it is issued at this time (<c>Again(after:)</c>).
/// </param>
[AkkaSerializable(Manifest = "durable-wf-repeated-v1", AllowEmpty = true)]
internal sealed record WfRepeated([property: AkkaField(0)] DateTimeOffset? Until = null) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-unexpected-v1", AllowEmpty = true)]
internal sealed record WfUnexpected : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-step-v1")]
internal readonly union WfStep(WfMoved, WfEnded, WfKept, WfRepeated, WfUnexpected) : IDurableEntityProtocol;

/// <param name="Fact">The author's fact.</param>
/// <param name="Answered">The position, in the work list of the entry it arrived in, of the work this fact answered.</param>
/// <param name="At">When the fact was decided.</param>
/// <param name="Step">What the table decided.</param>
/// <param name="FromHeld">True when the fact was taken from the front of the held list.</param>
[AkkaSerializable(Manifest = "durable-wf-fact-v1")]
internal sealed record WfFactRecorded(
    [property: AkkaField(0)] object Fact,
    [property: AkkaField(1)] int? Answered,
    [property: AkkaField(2)] DateTimeOffset At,
    [property: AkkaField(3)] WfStep Step,
    [property: AkkaField(4)] bool FromHeld) : IDurableEntityProtocol, IDurableJournalRecord;

/// <summary>A fact arrived while work was in flight. It is held, not decided.</summary>
/// <param name="Fact">The author's fact.</param>
[AkkaSerializable(Manifest = "durable-wf-held-v1")]
internal sealed record WfHeld([property: AkkaField(0)] object Fact) : IDurableEntityProtocol, IDurableJournalRecord;

/// <summary>
/// Attempt <paramref name="Attempt"/> of the work at <paramref name="Index"/> starts. Journaled before it runs: for
/// every attempt after the first of work with a retry policy, and for the first attempt after a delayed <c>Again</c>.
/// </summary>
/// <param name="Entry">The entry the work belongs to.</param>
/// <param name="Index">The work's position in the state's work list.</param>
/// <param name="Attempt">The number of the attempt that starts.</param>
/// <param name="At">When it starts.</param>
[AkkaSerializable(Manifest = "durable-wf-attempt-started-v1")]
internal sealed record WfAttemptStarted(
    [property: AkkaField(0)] int Entry,
    [property: AkkaField(1)] int Index,
    [property: AkkaField(2)] int Attempt,
    [property: AkkaField(3)] DateTimeOffset At) : IDurableEntityProtocol, IDurableJournalRecord;

/// <summary>
/// Attempt <paramref name="Attempt"/> of the work at <paramref name="Index"/> failed, or was cut off by a restart, and
/// its retry policy allows another. The next attempt is due at <paramref name="NextAt"/>.
/// </summary>
/// <param name="Entry">The entry the work belongs to.</param>
/// <param name="Index">The work's position in the state's work list.</param>
/// <param name="Attempt">The number of the attempt that failed.</param>
/// <param name="NextAt">When the next attempt is due.</param>
[AkkaSerializable(Manifest = "durable-wf-attempt-failed-v1")]
internal sealed record WfAttemptFailed(
    [property: AkkaField(0)] int Entry,
    [property: AkkaField(1)] int Index,
    [property: AkkaField(2)] int Attempt,
    [property: AkkaField(3)] DateTimeOffset NextAt) : IDurableEntityProtocol, IDurableJournalRecord;

[AkkaSerializable(Manifest = "durable-wf-event-v1")]
internal readonly union WfEvent(WfStarted, WfFactRecorded, WfHeld, WfAttemptStarted, WfAttemptFailed)
    : IDurableEntityProtocol, IDurableJournalRecord;

// ---- Commands and replies ----

[AkkaSerializable(Manifest = "durable-wf-start-v1")]
internal sealed record WfStart([property: AkkaField(0)] object Input)
    : IDurableEntityProtocol, IReplies<WfCommand, WfStartReply>
{
    public WfCommand ToCommand() => new(this);
}

/// <summary>The answer to a piece of the workflow's own work, or the notice that it got none.</summary>
/// <param name="Entry">The entry the work was issued for.</param>
/// <param name="Index">The work's position in the state's work list.</param>
/// <param name="Fact">The answer, or the give-up fact.</param>
/// <param name="Attempt">
/// For work with a retry policy: the attempt that answered. An answer of another attempt is dropped. 0 for work
/// without a policy.
/// </param>
[AkkaSerializable(Manifest = "durable-wf-answer-v1")]
internal sealed record WfAnswer(
    [property: AkkaField(0)] int Entry,
    [property: AkkaField(1)] int Index,
    [property: AkkaField(2)] object Fact,
    [property: AkkaField(3)] int Attempt = 0) : IDurableEntityProtocol;

/// <summary>
/// An attempt at work with a retry policy failed: the handler threw, ran out of time or answered with nothing. The
/// workflow attempts it again, or gives up when the policy has run out.
/// </summary>
/// <param name="Entry">The entry the work was issued for.</param>
/// <param name="Index">The work's position in the state's work list.</param>
/// <param name="Attempt">The attempt that failed.</param>
/// <param name="Reason">A <see cref="GaveUpReason"/>, stored as its number: <c>Failed</c> or <c>TimedOut</c>.</param>
/// <param name="Error">The failure's message.</param>
[AkkaSerializable(Manifest = "durable-wf-attempt-failure-v1")]
internal sealed record WfFailed(
    [property: AkkaField(0)] int Entry,
    [property: AkkaField(1)] int Index,
    [property: AkkaField(2)] int Attempt,
    [property: AkkaField(3)] int Reason,
    [property: AkkaField(4)] string? Error) : IDurableEntityProtocol;

/// <summary>A fact from outside the workflow.</summary>
/// <param name="Fact">The author's fact.</param>
/// <param name="WantsReply">
/// False for a message that an actor sent to the workflow's address: that actor does not expect a reply.
/// </param>
[AkkaSerializable(Manifest = "durable-wf-deliver-v1")]
internal sealed record WfDeliver(
    [property: AkkaField(0)] object Fact,
    [property: AkkaField(1)] bool WantsReply)
    : IDurableEntityProtocol, IReplies<WfCommand, WfDeliveryReply>
{
    public WfCommand ToCommand() => new(this);
}

/// <summary>The request to stop the workflow. The definition's <c>Cancel</c> function turns it into a fact.</summary>
/// <param name="Reason">Why the workflow should stop.</param>
/// <param name="By">
/// Who or what asked for the cancel. Use an id, such as a user id or a service name, not a person's name or
/// e-mail address, because the value goes into the journal and the log.
/// </param>
[AkkaSerializable(Manifest = "durable-wf-cancel-v1")]
internal sealed record WfCancel(
    [property: AkkaField(0)] string Reason,
    [property: AkkaField(1)] string By)
    : IDurableEntityProtocol, IReplies<WfCommand, WfDeliveryReply>
{
    public WfCommand ToCommand() => new(this);
}

/// <summary>
/// The payload of the workflow's reminder: decide the timers that are due, and let interrupted work give up.
/// </summary>
/// <param name="At">
/// The time the reminder was stored for. Akka.Reminders can deliver a reminder a little before its due time. A
/// timer due by this time is then decided at once, so an early delivery is not spent on a wake that does nothing.
/// </param>
[AkkaSerializable(Manifest = "durable-wf-wake-v1", AllowEmpty = true)]
internal sealed record WfWake([property: AkkaField(0)] DateTimeOffset? At = null) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-command-v1")]
internal readonly union WfCommand(WfStart, WfAnswer, WfDeliver, WfCancel, WfWake, WfFailed) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-start-accepted-v1", AllowEmpty = true)]
internal sealed record WfStartAccepted : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-start-refused-running-v1", AllowEmpty = true)]
internal sealed record WfStartRefusedRunning : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-start-refused-finished-v1")]
internal sealed record WfStartRefusedFinished([property: AkkaField(0)] object Outcome) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-start-reply-v1")]
internal readonly union WfStartReply(WfStartAccepted, WfStartRefusedRunning, WfStartRefusedFinished) : IDurableEntityProtocol;

// The answers to a fact from outside.

/// <summary>The fact was journaled and decided.</summary>
[AkkaSerializable(Manifest = "durable-wf-delivered-v1", AllowEmpty = true)]
internal sealed record WfDelivered : IDurableEntityProtocol;

/// <summary>The fact was journaled. It is decided when the work in flight settles.</summary>
[AkkaSerializable(Manifest = "durable-wf-held-reply-v1", AllowEmpty = true)]
internal sealed record WfHeldReply : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-not-started-reply-v1", AllowEmpty = true)]
internal sealed record WfNotStartedReply : IDurableEntityProtocol;

/// <summary>The workflow has finished and is closed.</summary>
[AkkaSerializable(Manifest = "durable-wf-closed-reply-v1")]
internal sealed record WfClosedReply([property: AkkaField(0)] object Outcome) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-delivery-reply-v1")]
internal readonly union WfDeliveryReply(WfDelivered, WfHeldReply, WfNotStartedReply, WfClosedReply)
    : IDurableEntityProtocol;

/// <summary>Stands in for the effect of work that was interrupted by a restart. Nothing runs for it.</summary>
internal sealed record InterruptedWork(object Work);

/// <summary>The workflow's one reminder.</summary>
internal static class WorkflowWake
{
    /// <summary>The reminder's key. Storing it again replaces it, so a workflow has at most one.</summary>
    public const string Key = "durable-workflow-wake";

    /// <summary>
    /// How long after <see cref="EntityOptions.EffectTimeout"/> the reminder wakes a workflow whose work has no
    /// answer. If the process died in the middle of a call, the wake activates the workflow, and the interrupted
    /// work gives up. A live workflow has its answer or its give-up by then, so the wake finds nothing to do.
    /// </summary>
    public static readonly TimeSpan BackstopMargin = TimeSpan.FromSeconds(5);
}

/// <summary>
/// Turns a <see cref="WorkflowDefinition{TInput,TState,TFact,TOutcome}"/> into the entity definition that hosts it.
/// </summary>
internal sealed class WorkflowEntity<TInput, TState, TFact, TOutcome>(
    WorkflowDefinition<TInput, TState, TFact, TOutcome> definition)
    where TInput : notnull
    where TState : notnull
    where TFact : notnull
    where TOutcome : notnull
{
    private static readonly WfStartReply Accepted = new WfStartAccepted();
    private static readonly WfStartReply RefusedRunning = new WfStartRefusedRunning();
    private static readonly WfDeliveryReply Delivered = new WfDelivered();
    private static readonly WfDeliveryReply HeldReply = new WfHeldReply();
    private static readonly WfDeliveryReply NotStarted = new WfNotStartedReply();

    private readonly EntityOptions _options = definition.Options ?? EntityOptions.Default;

    public EntityDefinition<WfState, WfCommand, WfEvent> Compile()
    {
        // A message of a type the definition accepts, sent to the workflow's address by an actor, becomes a fact.
        // That actor expects no reply. A message of any other type is refused.
        var accepted = ImmutableDictionary<Type, Func<object, WfCommand>>.Empty;
        foreach (var (type, wrap) in definition.Accepted)
            accepted = accepted.SetItem(type, message => new WfDeliver(wrap(message), WantsReply: false));

        return new EntityDefinition<WfState, WfCommand, WfEvent>(
            new EntityTypeName(definition.TypeName.Value),
            _ => new WfNotStarted(),
            Decide,
            WorkflowEvolve.Evolve,
            _options)
        {
            AfterRecovery = Resume,
            Accepted = accepted,
        };
    }

    /// <summary>
    /// Decides a command. A decision that journals something, or that handles a wake, also stores the reminder
    /// for the position the workflow is left in.
    /// </summary>
    private Decision<WfCommand, WfEvent> Decide(
        WfState state, WfCommand command, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        // A wake delivered early counts as arriving at the time its reminder was stored for.
        var reached = command is WfWake { At: { } at } && at > ctx.UtcNow ? at : ctx.UtcNow;

        var decision = DecideCommand(state, command, reached, ctx);
        if (decision is not Handled<WfCommand, WfEvent> handled || (command is not WfWake && handled.Events.IsDefaultOrEmpty))
            return decision;

        var after = handled.Events.IsDefaultOrEmpty ? state : handled.Events.Aggregate(state, WorkflowEvolve.Evolve);
        if (after is WfRunning running) return StoreWakeFirst(handled, running, reached, ctx);
        return state is WfRunning && after is WfFinished ? CancelWakeFirst(handled) : handled;
    }

    /// <summary>
    /// A decision that finishes the workflow cancels its reminder, ahead of the other effects. A cancel the
    /// reminder store refuses does not fail the finish: a wake left behind is ignored by a closed workflow.
    /// </summary>
    private static Handled<WfCommand, WfEvent> CancelWakeFirst(Handled<WfCommand, WfEvent> handled)
    {
        EntityEffect cancel = new CancelTimerEffect(WorkflowWake.Key, BestEffort: true);
        return handled with { Effects = handled.Effects.IsDefault ? [cancel] : handled.Effects.Insert(0, cancel) };
    }

    private Decision<WfCommand, WfEvent> DecideCommand(
        WfState state, WfCommand command, DateTimeOffset reached, DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        (state, command) switch
        {
            (WfNotStarted, WfStart start) => Begin(start, ctx),
            (WfRunning, WfStart start) => ctx.None().Reply(start, RefusedRunning),
            (WfFinished finished, WfStart start) =>
                ctx.None().Reply(start, (WfStartReply)new WfStartRefusedFinished(finished.Outcome)),

            (WfRunning running, WfAnswer answer) => Answer(running, answer, reached, ctx),

            // An answer for a workflow that has ended. A finished workflow is closed.
            (WfNotStarted or WfFinished, WfAnswer) => ctx.Ignore(),

            (WfRunning running, WfDeliver deliver) => deliver.Fact is TFact
                ? Deliver(running, deliver.Fact, deliver, deliver.WantsReply, reached, ctx)
                : ctx.Unhandled(),
            (WfRunning running, WfCancel cancel) => LogCancel(
                Deliver(running, definition.Cancel(new CancelRequest(cancel.Reason, cancel.By)), cancel, wantsReply: true,
                    reached, ctx),
                cancel, ctx),

            // A workflow that is not started, or has finished, refuses facts. Nothing is journaled.
            (WfNotStarted, WfDeliver deliver) => ReplyIf(ctx.None(), deliver, deliver.WantsReply, NotStarted),
            (WfNotStarted, WfCancel cancel) => ReplyIf(ctx.None(), cancel, wantsReply: true, NotStarted),
            (WfFinished finished, WfDeliver deliver) =>
                ReplyIf(ctx.None(), deliver, deliver.WantsReply, new WfClosedReply(finished.Outcome)),
            (WfFinished finished, WfCancel cancel) =>
                ReplyIf(ctx.None(), cancel, wantsReply: true, new WfClosedReply(finished.Outcome)),

            (WfRunning running, WfWake) => WakeUp(running, reached, ctx),

            // A stale wake: the workflow is not started, or it has finished and needs no wake.
            (WfNotStarted or WfFinished, WfWake) => ctx.Ignore(),

            (WfRunning running, WfFailed failed) => AttemptFailed(running, failed, reached, ctx),

            // A failed attempt of a workflow that has ended. Its retries ended with it.
            (WfNotStarted or WfFinished, WfFailed) => ctx.Ignore(),
        };

    private Handled<WfCommand, WfEvent> Begin(WfStart start, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        var input = (TInput)start.Input;
        var first = definition.Start(input);

        return Issue(
            ctx.Persist(new WfStarted(start.Input, first, ctx.UtcNow)).Reply(start, Accepted),
            ctx, input, first, entry: 1, enteredAt: ctx.UtcNow, delayed: null);
    }

    /// <summary>The answer to a piece of the workflow's own work, or the notice that it got none.</summary>
    private Decision<WfCommand, WfEvent> Answer(
        WfRunning running, WfAnswer answer, DateTimeOffset reached, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        if (answer.Entry == running.Entry)
        {
            // Work is issued once for each attempt, so a second answer to the same work is not expected.
            if (running.Settled.Contains(answer.Index)) return ctx.Ignore();

            // Work with a retry policy: only the attempt in flight answers. The answer of an earlier attempt that ran
            // past its time limit is dropped: one answer for each piece of work.
            if (answer.Attempt > 0 && Progress.Of(running).At(answer.Index) is var position
                && (position.Phase != WorkPhase.InFlight || position.Attempt != answer.Attempt))
                return ctx.Ignore();

            return Advance(running, [(answer.Fact, answer.Index)], reached, ctx, ctx.None());
        }

        // A late answer of work with a retry policy, for a state the workflow has left: the move ended its retries.
        if (answer.Attempt > 0) return ctx.Ignore();

        // A late answer, to work of an earlier entry. It settles nothing, so it is treated like a fact from outside.
        return InFlight(running, ctx)
            ? ctx.Persist(new WfHeld(answer.Fact))
            : Advance(running, [(answer.Fact, null)], reached, ctx, ctx.None());
    }

    /// <summary>A fact from outside: held while work is in flight, otherwise decided at once.</summary>
    private Handled<WfCommand, WfEvent> Deliver(
        WfRunning running,
        object fact,
        IReplies<WfCommand, WfDeliveryReply> request,
        bool wantsReply,
        DateTimeOffset reached,
        DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        InFlight(running, ctx)
            ? ReplyIf(ctx.Persist(new WfHeld(fact)), request, wantsReply, HeldReply)
            : Advance(running, [(fact, null)], reached, ctx, ReplyIf(ctx.None(), request, wantsReply, Delivered));

    /// <summary>
    /// The reminder arrived. Work whose next attempt is due by <paramref name="reached"/> starts, whether or not other
    /// work is in flight. While work is in flight nothing else is journaled: a due timer is decided when the work
    /// settles. At rest, the timers of the current entry that are due by <paramref name="reached"/> are decided.
    /// </summary>
    private Decision<WfCommand, WfEvent> WakeUp(
        WfRunning running, DateTimeOffset reached, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        var work = WorkOf(running, ctx);
        var startDue = EarliestDueStart(work, Progress.Of(running), reached) is not null;

        // A stale wake, a deadline of work still running here, or a timer that waits for work to settle. This
        // delivery is acknowledged, so a workflow that still needs a wake gets its reminder stored again.
        if (!startDue && (InFlight(running, ctx) || DueTimers(work, running.Settled, reached).Count == 0))
            return NextWake(running, reached, ctx) is null ? ctx.Ignore() : ctx.None();

        return Advance(running, [], reached, ctx, ctx.None());
    }

    /// <summary>
    /// An attempt at work with a retry policy failed. When the policy allows another attempt, the failure is journaled
    /// with the time the next attempt is due, and the work waits: it is not in flight, so held facts and due timers are
    /// decided. When the policy has run out, the workflow receives the give-up fact with
    /// <see cref="GaveUpReason.RetriesExhausted"/> and the number of attempts.
    /// </summary>
    private Decision<WfCommand, WfEvent> AttemptFailed(
        WfRunning running, WfFailed failed, DateTimeOffset reached, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        // The failure of an attempt that is not the one in flight: a state the workflow has left, or an earlier attempt.
        if (failed.Entry != running.Entry) return ctx.Ignore();
        var position = Progress.Of(running).At(failed.Index);
        if (position.Phase != WorkPhase.InFlight || position.Attempt != failed.Attempt) return ctx.Ignore();

        var work = WorkOf(running, ctx);
        if (failed.Index >= work.Length || work[failed.Index] is not ExecuteWork<TFact> execute) return ctx.Ignore();

        var reason = (GaveUpReason)failed.Reason;
        var policy = PolicyOf(execute, ctx);
        var (type, id, what, attempt) =
            (definition.TypeName.Value, ctx.EntityId.Value, execute.Effect.GetType().Name, failed.Attempt);

        if (policy is not null && policy.Allows(attempt + 1))
        {
            var nextAt = ctx.UtcNow + policy.WaitAfter(attempt);
            var record = new WfAttemptFailed(running.Entry, failed.Index, attempt, nextAt);
            var logged = ctx.Persist(record).Run<ILoggerFactory>((loggers, _) =>
            {
                loggers.CreateLogger("Akka.Durable.Workflow").LogWarning(
                    "Attempt {Attempt} of the work {Work} of workflow [{WorkflowType}/{WorkflowId}] failed ({Reason}: " +
                    "{Error}). Attempt {Next} is due at {NextAt}.", attempt, what, type, id, reason, failed.Error,
                    attempt + 1, nextAt);
                return Task.CompletedTask;
            });
            return Advance(WorkflowEvolve.Apply(running, record), [], reached, ctx, logged);
        }

        var gaveUp = new WorkGaveUp(execute.Effect, policy is null ? reason : GaveUpReason.RetriesExhausted,
            failed.Error, attempt);
        var told = ctx.None().Run<ILoggerFactory>((loggers, _) =>
        {
            loggers.CreateLogger("Akka.Durable.Workflow").LogWarning(
                "The work {Work} of workflow [{WorkflowType}/{WorkflowId}] gave up as {Reason} after {Attempts} " +
                "attempts. The workflow receives its give-up fact.", what, type, id, gaveUp.Reason, attempt);
            return Task.CompletedTask;
        });
        return Advance(running, [(definition.GaveUp(gaveUp), failed.Index)], reached, ctx, told);
    }

    /// <summary>Logs a cancel that reached a running workflow, whether it was decided at once or held.</summary>
    private Handled<WfCommand, WfEvent> LogCancel(
        Handled<WfCommand, WfEvent> handled, WfCancel cancel, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        var (type, id) = (definition.TypeName.Value, ctx.EntityId.Value);
        return handled.Run<ILoggerFactory>((loggers, _) =>
        {
            loggers.CreateLogger("Akka.Durable.Workflow").LogInformation(
                "Workflow [{WorkflowType}/{WorkflowId}] was asked to cancel by {By}. Reason: {Reason}",
                type, id, cancel.By, cancel.Reason);
            return Task.CompletedTask;
        });
    }

    private static Handled<WfCommand, WfEvent> ReplyIf(
        Handled<WfCommand, WfEvent> handled,
        IReplies<WfCommand, WfDeliveryReply> request,
        bool wantsReply,
        WfDeliveryReply reply) =>
        wantsReply ? handled.Reply(request, reply) : handled;

    /// <summary>
    /// True when a piece of the current entry's work has an attempt with no answer yet. Timers do not count: a wait
    /// holds nothing. Work that waits for its next attempt does not count either. The state a workflow is in always has
    /// its work issued: a state and its work come from one decision.
    /// </summary>
    private bool InFlight(WfRunning running, DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        HasWorkInFlight(WorkOf(running, ctx), Progress.Of(running));

    private static bool HasWorkInFlight(ImmutableArray<Work<TFact>> work, Progress progress)
    {
        for (var position = 0; position < work.Length; position++)
            if (work[position] is ExecuteWork<TFact> && progress.At(position).Phase == WorkPhase.InFlight)
                return true;

        return false;
    }

    /// <summary>The earliest time, by <paramref name="reached"/>, at which a piece of work waits for an attempt to start.</summary>
    private static DateTimeOffset? EarliestDueStart(
        ImmutableArray<Work<TFact>> work, Progress progress, DateTimeOffset reached)
    {
        DateTimeOffset? earliest = null;
        for (var index = 0; index < work.Length; index++)
            if (work[index] is ExecuteWork<TFact> && progress.At(index) is { Phase: WorkPhase.Waiting } waiting
                && waiting.At <= reached)
                earliest = Earliest(earliest, waiting.At);

        return earliest;
    }

    /// <summary>
    /// The retry policy registered with the handler of <paramref name="work"/>. Null for a send to an entity, for a
    /// handler registered without one, and outside a host.
    /// </summary>
    private static RetryPolicy? PolicyOf(ExecuteWork<TFact> work, DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        work.EffectType is { } type && ctx.Routes is { } routes && routes.TryGet(type, out var route)
        && route.OutcomeType == typeof(TFact)
            ? route.Policy
            : null;

    /// <summary>The timers in <paramref name="work"/> that are due by <paramref name="reached"/> and not settled, in list order.</summary>
    private static List<(int Position, TimerWork<TFact> Timer)> DueTimers(
        ImmutableArray<Work<TFact>> work, ImmutableArray<int> settled, DateTimeOffset reached)
    {
        var due = new List<(int, TimerWork<TFact>)>();
        for (var position = 0; position < work.Length; position++)
            if (work[position] is TimerWork<TFact> timer && timer.Due <= reached && !settled.Contains(position))
                due.Add((position, timer));

        return due;
    }

    /// <summary>
    /// When the workflow next needs a wake: the earliest of the times below. Work that waits for its next attempt, or
    /// for a delayed <c>Again</c>: when that attempt is due. While an attempt has no answer: the time by which it must
    /// have answered or given up; timers do not count then, because a timer is decided only once the work settles. At
    /// rest: the earliest unsettled timer. Null when nothing needs a wake: a workflow that only waits for facts from
    /// outside has no reminder.
    /// </summary>
    /// <param name="reached">The time the decision counts as: now, or the time an early wake was stored for.</param>
    private DateTimeOffset? NextWake(
        WfRunning running, DateTimeOffset reached, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        var work = WorkOf(running, ctx);
        var progress = Progress.Of(running);
        DateTimeOffset? backstop = null;
        DateTimeOffset? wake = null;
        for (var position = 0; position < work.Length; position++)
        {
            if (work[position] is not ExecuteWork<TFact>) continue;
            var at = progress.At(position);
            if (at.Phase == WorkPhase.Waiting)
                wake = Earliest(wake, at.At);
            else if (at.Phase == WorkPhase.InFlight)
            {
                // Work that is still in flight past this time is running in this process: look again a margin later.
                var deadline = at.At + _options.EffectTimeout + WorkflowWake.BackstopMargin;
                backstop = Earliest(backstop, deadline > reached ? deadline : ctx.UtcNow + WorkflowWake.BackstopMargin);
            }
        }

        if (backstop is { } inFlight) return Earliest(wake, inFlight);

        for (var position = 0; position < work.Length; position++)
            if (work[position] is TimerWork<TFact> timer && !running.Settled.Contains(position))
                wake = Earliest(wake, timer.Due);

        return wake;
    }

    private static DateTimeOffset Earliest(DateTimeOffset? current, DateTimeOffset candidate) =>
        current is { } known && known <= candidate ? known : candidate;

    /// <summary>
    /// Puts the reminder for <paramref name="running"/> first among the decision's effects. The events are
    /// journaled before any effect runs, so the reminder is replaced only once the journal has the new position.
    /// Work and replies come after it: if it cannot be stored, they do not happen.
    /// </summary>
    private Handled<WfCommand, WfEvent> StoreWakeFirst(
        Handled<WfCommand, WfEvent> handled,
        WfRunning running,
        DateTimeOffset reached,
        DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        if (NextWake(running, reached, ctx) is not { } wake) return handled;

        EntityEffect store = new ScheduleEffect(
            WorkflowWake.Key, wake > ctx.UtcNow ? wake - ctx.UtcNow : TimeSpan.Zero, (WfCommand)new WfWake(wake));
        return handled with { Effects = handled.Effects.IsDefault ? [store] : handled.Effects.Insert(0, store) };
    }

    /// <summary>
    /// Decides the given facts, then each held fact in arrival order while no work is in flight, then, once, the
    /// due timers of the position reached if it is at rest, then starts the attempts that are due in the entry the
    /// workflow started in, and journals them all in one write. The work of the state the workflow ends in is issued
    /// last, once.
    /// </summary>
    /// <param name="facts">
    /// The facts to decide, each with the position of the work it answers when it answers current work. Empty for
    /// a wake, which decides only timers and starts due attempts.
    /// </param>
    /// <param name="reached">The time the decision counts as: now, or the time an early wake was stored for.</param>
    private Handled<WfCommand, WfEvent> Advance(
        WfRunning running,
        IReadOnlyList<(object Fact, int? Answered)> facts,
        DateTimeOffset reached,
        DecisionContext<WfState, WfCommand, WfEvent> ctx,
        Handled<WfCommand, WfEvent> handled)
    {
        var input = (TInput)running.Input;
        var state = (TState)running.State;
        var entry = running.Entry;
        var enteredAt = running.EnteredAt;
        var progress = Progress.Of(running);
        var held = running.Held;
        var issued = true;
        var finished = false;
        var events = new List<WfEvent>();
        var unexpected = new List<(TFact Fact, TState State)>();

        void Step(object value, int? position, bool fromHeld)
        {
            var fact = (TFact)value;
            var transition = definition.Decide(
                state, fact, new DecideContext<TInput, TState, TOutcome>(IdOf(ctx), input, ctx.UtcNow, enteredAt));

            void Record(WfStep step) => events.Add(new WfFactRecorded(value, position, ctx.UtcNow, step, fromHeld));

            switch (transition)
            {
                case NextTransition<TState> next:
                    Record(new WfMoved(next.State));
                    (state, entry, enteredAt, progress, issued) =
                        (next.State, entry + 1, ctx.UtcNow, Progress.Fresh(ctx.UtcNow, delayed: null), false);
                    break;

                case AgainTransition again:
                    // A delayed repeat: the work of the new entry is issued once the delay has passed.
                    var until = again.After is { } after ? ctx.UtcNow + after : (DateTimeOffset?)null;
                    Record(new WfRepeated(until));
                    (entry, progress, issued) = (entry + 1, Progress.Fresh(ctx.UtcNow, until), false);
                    break;

                case FinishTransition<TOutcome> finish:
                    Record(new WfEnded(finish.Outcome));
                    finished = true;
                    break;

                case StayTransition:
                    Record(new WfKept());
                    if (position is { } kept) progress = progress.Settle(kept);
                    break;

                case UnexpectedTransition:
                    Record(new WfUnexpected());
                    if (position is { } index) progress = progress.Settle(index);
                    unexpected.Add((fact, state));
                    break;

                default:
                    throw new InvalidOperationException(
                        $"{definition.TypeName.Value}.Decide returned an uninitialized Transition for the fact {fact} " +
                        $"in the state {state}.");
            }
        }

        ImmutableArray<Work<TFact>> Current() => WorkOf(state, ctx, input, enteredAt);
        bool WorkInFlight() => issued && HasWorkInFlight(Current(), progress);

        foreach (var (fact, answered) in facts)
            Step(fact, answered, fromHeld: false);

        while (!finished && !held.IsEmpty && !WorkInFlight())
        {
            var next = held[0];
            held = held.RemoveAt(0);
            Step(next, position: null, fromHeld: true);
        }

        // One pass over the timers of the position reached, if it is at rest. A timer that moves the workflow on
        // ends the pass: a timer of the next position that is already due gets an immediate wake instead. An attempt
        // that was due before the first due timer starts first, and the timers wait for its answer.
        if (!finished && !WorkInFlight())
        {
            var at = entry;
            var timers = DueTimers(Current(), progress.Settled, reached);
            var attemptFirst = issued && timers.Count > 0
                && EarliestDueStart(Current(), progress, reached) is { } start && start < timers.Min(t => t.Timer.Due);
            if (!attemptFirst)
                foreach (var (position, timer) in timers)
                {
                    if (finished || entry != at) break;
                    Step(timer.Fact, position, fromHeld: false);
                }
        }

        // Work of the entry the workflow is still in whose next attempt is due: each attempt is journaled before it runs.
        var starts = new List<(ExecuteWork<TFact> Work, int Index, int Attempt)>();
        if (!finished && issued)
        {
            var work = Current();
            for (var index = 0; index < work.Length; index++)
            {
                if (work[index] is not ExecuteWork<TFact> execute
                    || progress.At(index) is not { Phase: WorkPhase.Waiting } waiting || waiting.At > reached)
                    continue;

                events.Add(new WfAttemptStarted(entry, index, waiting.Attempt, ctx.UtcNow));
                progress = progress.Start(index, waiting.Attempt, ctx.UtcNow);
                starts.Add((execute, index, waiting.Attempt));
            }
        }

        handled = handled.Persist([.. events]);

        var (type, id) = (definition.TypeName.Value, ctx.EntityId.Value);
        foreach (var (fact, inState) in unexpected)
            handled = handled.Run<ILoggerFactory>((loggers, _) =>
            {
                loggers.CreateLogger("Akka.Durable.Workflow").LogWarning(
                    "Workflow [{WorkflowType}/{WorkflowId}] received the fact {Fact} in the state {State}, " +
                    "which its table marks as unexpected. The state is unchanged.", type, id, fact, inState);
                return Task.CompletedTask;
            });

        foreach (var (work, index, attempt) in starts)
            handled = handled.Add(Execute(work, entry, index, attempt, ctx));

        // A state that was entered here has not had its work issued yet. Only the last one gets it.
        return finished || issued ? handled : Issue(handled, ctx, input, state, entry, enteredAt, progress.Delayed);
    }

    /// <summary>
    /// After a restart: every piece of the state's work whose attempt has no answer was interrupted. Work without a
    /// retry policy is not issued again: the workflow receives the give-up fact with
    /// <see cref="GaveUpReason.Interrupted"/>, and the table decides. Work with a policy counts the cut-off attempt as
    /// failed: its next attempt is journaled with the time it is due, and it starts then; when the policy has run out,
    /// the workflow receives the give-up fact with <see cref="GaveUpReason.RetriesExhausted"/>. Work whose next attempt
    /// came due while the process was down starts now. Then the reminder for the recovered position is stored again, in
    /// case the process died between a journal write and the reminder write.
    /// </summary>
    private Handled<WfCommand, WfEvent> Resume(WfState state, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        if (state is not WfRunning running) return ctx.None();

        var now = ctx.UtcNow;
        var work = WorkOf(running, ctx);
        var progress = Progress.Of(running);
        var handled = ctx.None();
        var after = running;
        var answers = new List<(object Effect, object Answer)>();
        for (var index = 0; index < work.Length; index++)
        {
            if (work[index] is not ExecuteWork<TFact> execute || progress.At(index) is not { Phase: WorkPhase.InFlight } cut)
                continue;

            var effect = execute.Effect;
            var policy = PolicyOf(execute, ctx);
            if (policy is null)
                answers.Add((effect, (WfCommand)new WfAnswer(running.Entry, index,
                    definition.GaveUp(new WorkGaveUp(effect, GaveUpReason.Interrupted, Attempts: cut.Attempt)))));
            else if (policy.Allows(cut.Attempt + 1))
            {
                // The cut-off attempt may still have been running up to its time limit, so the wait counts from there.
                var record = new WfAttemptFailed(running.Entry, index, cut.Attempt,
                    cut.At + _options.EffectTimeout + policy.WaitAfter(cut.Attempt));
                handled = handled.Persist(record);
                after = WorkflowEvolve.Apply(after, record);
            }
            else
                answers.Add((effect, (WfCommand)new WfAnswer(running.Entry, index,
                    definition.GaveUp(new WorkGaveUp(effect, GaveUpReason.RetriesExhausted, Attempts: cut.Attempt)),
                    cut.Attempt)));
        }

        if (!handled.Events.IsDefaultOrEmpty || EarliestDueStart(work, Progress.Of(after), now) is not null)
            handled = Advance(after, [], now, ctx, handled);

        var reached = handled.Events.IsDefaultOrEmpty
            ? (WfState)running
            : handled.Events.Aggregate((WfState)running, WorkflowEvolve.Evolve);
        if (reached is WfRunning position) handled = StoreWakeFirst(handled, position, now, ctx);
        else if (reached is WfFinished) handled = CancelWakeFirst(handled);

        // Nothing runs: the notice comes back to the workflow through the path every answer takes.
        foreach (var (effect, answer) in answers)
            handled = handled.Add(new ExecuteEffect(new InterruptedWork(effect))
            {
                Resolve = _ => _ => Task.FromResult<object?>(answer),
            });

        return handled;
    }

    /// <summary>
    /// Adds the work of <paramref name="state"/> to a decision, to run once its events are journaled. Timers are
    /// not issued: the reminder covers them. After a delayed <c>Again</c> nothing is issued yet: the work starts when
    /// the delay has passed.
    /// </summary>
    private Handled<WfCommand, WfEvent> Issue(
        Handled<WfCommand, WfEvent> handled,
        DecisionContext<WfState, WfCommand, WfEvent> ctx,
        TInput input,
        TState state,
        int entry,
        DateTimeOffset enteredAt,
        DateTimeOffset? delayed)
    {
        if (delayed is not null) return handled;

        var work = WorkOf(state, ctx, input, enteredAt);
        for (var index = 0; index < work.Length; index++)
            if (work[index] is ExecuteWork<TFact> execute)
                handled = handled.Add(Execute(execute, entry, index, attempt: 1, ctx));

        return handled;
    }

    /// <summary>
    /// The id of the work at <paramref name="index"/> of entry <paramref name="entry"/>: the same each time it is asked
    /// for, on every attempt, because the entry and the position are journaled.
    /// </summary>
    private EffectDelivery DeliveryOf(
        int entry, int index, int attempt, DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        new(new EffectId($"{definition.TypeName.Value}/{ctx.EntityId.Value}/{entry}.{index}", entry), attempt);

    private ImmutableArray<Work<TFact>> WorkOf(WfRunning running, DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        WorkOf((TState)running.State, ctx, (TInput)running.Input, running.EnteredAt);

    private ImmutableArray<Work<TFact>> WorkOf(
        TState state, DecisionContext<WfState, WfCommand, WfEvent> ctx, TInput input, DateTimeOffset enteredAt)
    {
        var list = definition.Work(state, new WorkContext<TInput, TFact>(IdOf(ctx), input, enteredAt))
                   ?? throw new InvalidOperationException(
                       $"{definition.TypeName.Value}.Work returned null for the state {state}. " +
                       "Return ctx.Nothing() for a state that has no work.");
        return list.Items.IsDefault ? [] : list.Items;
    }

    /// <summary>
    /// Attempt <paramref name="attempt"/> of a piece of work. Without a retry policy, a failure or a time-out is an
    /// answer: the give-up fact. With one, it comes back as a <see cref="WfFailed"/>, and the workflow attempts the work
    /// again or gives up.
    /// </summary>
    private ExecuteEffect Execute(
        ExecuteWork<TFact> work, int entry, int index, int attempt, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        if (work.Resolve is null)
            throw new InvalidOperationException("Work must be created through WorkContext.Execute.");

        var delivery = DeliveryOf(entry, index, attempt, ctx);
        var retried = PolicyOf(work, ctx) is not null;
        var answered = retried ? attempt : 0;
        return new ExecuteEffect(work.Effect)
        {
            // The handler returns one of the author's facts. The entity's command is the answer that carries it.
            Resolve = services =>
            {
                var invoke = work.Resolve(services, delivery);
                return async ct =>
                    (WfCommand)new WfAnswer(entry, index, FactOf(await invoke(ct), work.Effect), answered);
            },

            Failed = failure =>
            {
                var reason = failure is TimeoutException ? GaveUpReason.TimedOut : GaveUpReason.Failed;

                // Without a policy, a failure or a timeout is an answer too: the fact the definition names for work
                // that gave up.
                return retried
                    ? (WfCommand)new WfFailed(entry, index, attempt, (int)reason, failure.Message)
                    : (WfCommand)new WfAnswer(entry, index,
                        definition.GaveUp(new WorkGaveUp(work.Effect, reason, failure.Message, attempt)));
            },
        };
    }

    /// <summary>
    /// Work must answer with a fact. A null, or a union that holds no value, cannot be decided by the table, so it
    /// is a failure of the work like any other and the workflow receives the give-up fact.
    /// </summary>
    private static TFact FactOf(TFact fact, object work) =>
        fact is null or IUnion { Value: null }
            ? throw new InvalidOperationException($"The work {work} answered with no fact.")
            : fact;

    private static WorkflowId IdOf(DecisionContext<WfState, WfCommand, WfEvent> ctx) => new(ctx.EntityId.Value);
}

/// <summary>Where a piece of work stands.</summary>
internal enum WorkPhase
{
    /// <summary>Answered during this entry.</summary>
    Settled,

    /// <summary>An attempt runs and has no answer yet.</summary>
    InFlight,

    /// <summary>The next attempt is due later: after a failed attempt, or after a delayed <c>Again</c>.</summary>
    Waiting,
}

/// <param name="Phase">Where the work stands.</param>
/// <param name="Attempt">In flight: the attempt that runs. Waiting: the attempt that is due.</param>
/// <param name="At">In flight: when the attempt started. Waiting: when the attempt is due.</param>
internal readonly record struct WorkPosition(WorkPhase Phase, int Attempt, DateTimeOffset At);

/// <summary>The answers and attempts of the work of one entry: what <see cref="WfRunning"/> records about them.</summary>
internal readonly record struct Progress(
    ImmutableArray<int> Settled, ImmutableArray<object> Attempts, DateTimeOffset? Delayed, DateTimeOffset IssuedAt)
{
    public static Progress Of(WfRunning running) =>
        new(running.Settled, WorkAttempts.Of(running.Attempts), running.Delayed, running.IssuedAt);

    /// <summary>A new entry, issued at <paramref name="issuedAt"/>, or waiting until <paramref name="delayed"/>.</summary>
    public static Progress Fresh(DateTimeOffset issuedAt, DateTimeOffset? delayed) => new([], [], delayed, issuedAt);

    /// <summary>
    /// Where the work at <paramref name="index"/> stands. Without a recorded attempt it is at attempt 1, started when
    /// the entry's work was issued, or waiting for a delayed <c>Again</c>.
    /// </summary>
    public WorkPosition At(int index)
    {
        if (Settled.Contains(index)) return new(WorkPhase.Settled, 0, default);
        if (WorkAttempts.Find(Attempts, index) is { } recorded)
            return recorded.NextAt is { } next
                ? new(WorkPhase.Waiting, recorded.Attempt + 1, next)
                : new(WorkPhase.InFlight, recorded.Attempt, recorded.StartedAt);
        return Delayed is { } due ? new(WorkPhase.Waiting, 1, due) : new(WorkPhase.InFlight, 1, IssuedAt);
    }

    public Progress Settle(int index) => this with { Settled = Settled.Add(index) };

    public Progress Start(int index, int attempt, DateTimeOffset at) =>
        this with { Attempts = WorkAttempts.Set(Attempts, new WfAttempt(index, attempt, at, NextAt: null)) };
}

/// <summary>Reads and changes the list of <see cref="WfAttempt"/> in a <see cref="WfRunning"/>.</summary>
internal static class WorkAttempts
{
    /// <summary>The list, empty when it was not written: a state journaled before work had attempts.</summary>
    public static ImmutableArray<object> Of(ImmutableArray<object> attempts) => attempts.IsDefault ? [] : attempts;

    public static WfAttempt? Find(ImmutableArray<object> attempts, int index)
    {
        foreach (var item in Of(attempts))
            if (item is WfAttempt attempt && attempt.Index == index)
                return attempt;

        return null;
    }

    public static ImmutableArray<object> Set(ImmutableArray<object> attempts, WfAttempt attempt)
    {
        var list = Of(attempts);
        for (var i = 0; i < list.Length; i++)
            if (list[i] is WfAttempt existing && existing.Index == attempt.Index)
                return list.SetItem(i, attempt);

        return list.Add(attempt);
    }
}

internal static class WorkflowEvolve
{
    public static WfState Evolve(WfState state, WfEvent @event) =>
        (state, @event) switch
        {
            (WfNotStarted, WfStarted started) =>
                new WfRunning(started.Input, started.First, 1, started.At, [], [], started.At, []),
            (WfRunning running, WfHeld held) =>
                running with { Held = running.Held.Add(held.Fact) },
            // A fact taken from the held list leaves it, then its step applies as for any fact.
            (WfRunning running, WfFactRecorded { FromHeld: true } recorded) =>
                Apply(running with { Held = running.Held.RemoveAt(0) }, recorded),
            (WfRunning running, WfFactRecorded recorded) => Apply(running, recorded),
            (WfRunning running, WfAttemptStarted started) => Apply(running, started),
            (WfRunning running, WfAttemptFailed failed) => Apply(running, failed),
            _ => throw new InvalidOperationException(
                $"The journal holds the workflow event {@event.Value} for a workflow in the state {state.Value}. " +
                "That event cannot follow that state, so the journal is not one this workflow wrote."),
        };

    /// <summary>An attempt starts: the work is in flight again.</summary>
    internal static WfRunning Apply(WfRunning running, WfAttemptStarted started) =>
        started.Entry != running.Entry
            ? running
            : running with
            {
                Attempts = WorkAttempts.Set(running.Attempts,
                    new WfAttempt(started.Index, started.Attempt, started.At, NextAt: null)),
            };

    /// <summary>An attempt failed: the work waits for its next attempt.</summary>
    internal static WfRunning Apply(WfRunning running, WfAttemptFailed failed)
    {
        if (failed.Entry != running.Entry) return running;
        var startedAt = WorkAttempts.Find(running.Attempts, failed.Index)?.StartedAt ?? running.IssuedAt;
        return running with
        {
            Attempts = WorkAttempts.Set(running.Attempts,
                new WfAttempt(failed.Index, failed.Attempt, startedAt, failed.NextAt)),
        };
    }

    // A move and a repeat issue the work of the new entry, so they set the time the work was issued. They start the
    // count of attempts again.
    private static WfState Apply(WfRunning running, WfFactRecorded recorded) =>
        recorded.Step switch
        {
            WfMoved moved => running with
            {
                State = moved.Next, Entry = running.Entry + 1, EnteredAt = recorded.At, Settled = [],
                IssuedAt = recorded.At, Attempts = [], Delayed = null,
            },
            WfRepeated repeated => running with
            {
                Entry = running.Entry + 1, Settled = [], IssuedAt = recorded.At, Attempts = [], Delayed = repeated.Until,
            },
            WfEnded ended => new WfFinished(running.Input, ended.Outcome, recorded.At),
            WfKept or WfUnexpected => recorded.Answered is { } index
                ? running with { Settled = running.Settled.Add(index) }
                : running,
        };
}
