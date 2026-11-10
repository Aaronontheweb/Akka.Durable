using System.Collections.Immutable;
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
/// <param name="Settled">Positions in the state's work list that have been answered during this entry.</param>
/// <param name="Held">
/// Facts that arrived while work was in flight, in arrival order. They are decided when the work settles.
/// </param>
[AkkaSerializable(Manifest = "durable-wf-running-v1")]
internal sealed record WfRunning(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object State,
    [property: AkkaField(2)] int Entry,
    [property: AkkaField(3)] DateTimeOffset EnteredAt,
    [property: AkkaField(4)] ImmutableArray<int> Settled,
    [property: AkkaField(5)] ImmutableArray<WfHeldFact> Held) : IDurableEntityProtocol
{
    /// <summary>The held facts. A state written before this field existed has none.</summary>
    public ImmutableArray<WfHeldFact> HeldFacts => Held.IsDefault ? [] : Held;

    // ImmutableArray compares by reference; compare its contents so records with equal data are equal.
    public bool Equals(WfRunning? other) =>
        other is not null && Equals(Input, other.Input) && Equals(State, other.State) && Entry == other.Entry
        && EnteredAt == other.EnteredAt && Settled.SequenceEqual(other.Settled)
        && HeldFacts.SequenceEqual(other.HeldFacts);

    public override int GetHashCode() =>
        HashCode.Combine(Input, State, Entry, EnteredAt, Settled.Length, HeldFacts.Length);
}

/// <summary>A fact that arrived while work was in flight.</summary>
/// <param name="Fact">The author's fact.</param>
/// <param name="At">When it arrived.</param>
[AkkaSerializable(Manifest = "durable-wf-held-fact-v1")]
internal sealed record WfHeldFact(
    [property: AkkaField(0)] object Fact,
    [property: AkkaField(1)] DateTimeOffset At) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-finished-v1")]
internal sealed record WfFinished(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object Outcome,
    [property: AkkaField(2)] OutcomeKind Kind,
    [property: AkkaField(3)] DateTimeOffset At) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-state-v1")]
internal readonly union WfState(WfNotStarted, WfRunning, WfFinished) : IDurableEntityProtocol;

// ---- Events: the start, then each fact with what it led to ----

[AkkaSerializable(Manifest = "durable-wf-started-v1")]
internal sealed record WfStarted(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object First,
    [property: AkkaField(2)] DateTimeOffset At) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-moved-v1")]
internal sealed record WfMoved([property: AkkaField(0)] object Next) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-ended-v1")]
internal sealed record WfEnded(
    [property: AkkaField(0)] object Outcome,
    [property: AkkaField(1)] OutcomeKind Kind) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-kept-v1", AllowEmpty = true)]
internal sealed record WfKept : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-repeated-v1", AllowEmpty = true)]
internal sealed record WfRepeated : IDurableEntityProtocol;

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
    [property: AkkaField(4)] bool FromHeld) : IDurableEntityProtocol;

/// <summary>A fact arrived while work was in flight. It is held, not decided.</summary>
/// <param name="Fact">The author's fact.</param>
/// <param name="At">When it arrived.</param>
[AkkaSerializable(Manifest = "durable-wf-held-v1")]
internal sealed record WfHeld(
    [property: AkkaField(0)] object Fact,
    [property: AkkaField(1)] DateTimeOffset At) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-event-v1")]
internal readonly union WfEvent(WfStarted, WfFactRecorded, WfHeld) : IDurableEntityProtocol;

// ---- Commands and replies ----

[AkkaSerializable(Manifest = "durable-wf-start-v1")]
internal sealed record WfStart([property: AkkaField(0)] object Input)
    : IDurableEntityProtocol, IReplies<WfCommand, WfStartReply>
{
    public WfCommand ToCommand() => new(this);
}

/// <summary>The answer to a piece of the workflow's own work, or the notice that it got none.</summary>
[AkkaSerializable(Manifest = "durable-wf-answer-v1")]
internal sealed record WfAnswer(
    [property: AkkaField(0)] int Entry,
    [property: AkkaField(1)] int Index,
    [property: AkkaField(2)] object Fact) : IDurableEntityProtocol;

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
[AkkaSerializable(Manifest = "durable-wf-cancel-v1")]
internal sealed record WfCancel(
    [property: AkkaField(0)] string Reason,
    [property: AkkaField(1)] string? By)
    : IDurableEntityProtocol, IReplies<WfCommand, WfDeliveryReply>
{
    public WfCommand ToCommand() => new(this);
}

[AkkaSerializable(Manifest = "durable-wf-command-v1")]
internal readonly union WfCommand(WfStart, WfAnswer, WfDeliver, WfCancel) : IDurableEntityProtocol;

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

    public EntityDefinition<WfState, WfCommand, WfEvent> Compile()
    {
        var options = definition.Options ?? WorkflowOptions.Default;

        // A message sent to the workflow's address by an actor is a fact. That actor expects no reply.
        var accepted = ImmutableDictionary<Type, Func<object, WfCommand>>.Empty
            .SetItem(typeof(TFact), message => new WfDeliver(message, WantsReply: false));
        foreach (var (type, wrap) in definition.Accepted)
            accepted = accepted.SetItem(type, message => new WfDeliver(wrap(message), WantsReply: false));

        return new EntityDefinition<WfState, WfCommand, WfEvent>(
            new EntityTypeName(definition.TypeName.Value),
            _ => new WfNotStarted(),
            Decide,
            WorkflowEvolve.Evolve,
            options.Hosting with { EffectTimeout = options.AttemptLimit })
        {
            Resume = Resume,
            Accepted = accepted,
        };
    }

    private Decision<WfCommand, WfEvent> Decide(
        WfState state, WfCommand command, DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        (state, command) switch
        {
            (WfNotStarted, WfStart start) => Begin(start, ctx),
            (WfRunning, WfStart start) => ctx.None().Reply(start, RefusedRunning),
            (WfFinished finished, WfStart start) =>
                ctx.None().Reply(start, (WfStartReply)new WfStartRefusedFinished(finished.Outcome)),

            (WfRunning running, WfAnswer answer) => Answer(running, answer, ctx),

            // An answer for a workflow that has ended. A finished workflow is closed.
            (WfNotStarted or WfFinished, WfAnswer) => ctx.Ignore(),

            (WfRunning running, WfDeliver deliver) => deliver.Fact is TFact
                ? Deliver(running, deliver.Fact, deliver, deliver.WantsReply, ctx)
                : ctx.Unhandled(),
            (WfRunning running, WfCancel cancel) =>
                Deliver(running, definition.Cancel(new CancelRequest(cancel.Reason, cancel.By)), cancel, wantsReply: true, ctx),

            // A workflow that is not started, or has finished, refuses facts. Nothing is journaled.
            (WfNotStarted, WfDeliver deliver) => ReplyIf(ctx.None(), deliver, deliver.WantsReply, NotStarted),
            (WfNotStarted, WfCancel cancel) => ReplyIf(ctx.None(), cancel, wantsReply: true, NotStarted),
            (WfFinished finished, WfDeliver deliver) =>
                ReplyIf(ctx.None(), deliver, deliver.WantsReply, new WfClosedReply(finished.Outcome)),
            (WfFinished finished, WfCancel cancel) =>
                ReplyIf(ctx.None(), cancel, wantsReply: true, new WfClosedReply(finished.Outcome)),
        };

    private Handled<WfCommand, WfEvent> Begin(WfStart start, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        var input = (TInput)start.Input;
        var first = definition.Start(input);

        return Issue(
            ctx.Persist(new WfStarted(start.Input, first, ctx.UtcNow)).Reply(start, Accepted),
            ctx, input, first, entry: 1, enteredAt: ctx.UtcNow);
    }

    /// <summary>The answer to a piece of the workflow's own work, or the notice that it got none.</summary>
    private Decision<WfCommand, WfEvent> Answer(
        WfRunning running, WfAnswer answer, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        if (answer.Entry == running.Entry)
        {
            // Work is issued once for each entry, so a second answer to the same work is not expected.
            if (running.Settled.Contains(answer.Index)) return ctx.Ignore();
            return Advance(running, answer.Fact, answer.Index, ctx, ctx.None());
        }

        // A late answer, to work of an earlier entry. It settles nothing, so it is treated like a fact from outside.
        return InFlight(running, ctx)
            ? ctx.Persist(new WfHeld(answer.Fact, ctx.UtcNow))
            : Advance(running, answer.Fact, answered: null, ctx, ctx.None());
    }

    /// <summary>A fact from outside: held while work is in flight, otherwise decided at once.</summary>
    private Handled<WfCommand, WfEvent> Deliver(
        WfRunning running,
        object fact,
        IReplies<WfCommand, WfDeliveryReply> request,
        bool wantsReply,
        DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        InFlight(running, ctx)
            ? ReplyIf(ctx.Persist(new WfHeld(fact, ctx.UtcNow)), request, wantsReply, HeldReply)
            : Advance(running, fact, answered: null, ctx, ReplyIf(ctx.None(), request, wantsReply, Delivered));

    private static Handled<WfCommand, WfEvent> ReplyIf(
        Handled<WfCommand, WfEvent> handled,
        IReplies<WfCommand, WfDeliveryReply> request,
        bool wantsReply,
        WfDeliveryReply reply) =>
        wantsReply ? handled.Reply(request, reply) : handled;

    /// <summary>
    /// True when a piece of the current entry's work has no answer yet. The state a workflow is in always has
    /// its work issued: a state and its work come from one decision.
    /// </summary>
    private bool InFlight(WfRunning running, DecisionContext<WfState, WfCommand, WfEvent> ctx) =>
        WorkOf((TState)running.State, ctx, (TInput)running.Input, running.EnteredAt).Length > running.Settled.Length;

    /// <summary>
    /// Decides a fact, then each held fact in arrival order while no work is in flight, and journals them all
    /// in one write. The work of the state the workflow ends in is issued last, once.
    /// </summary>
    /// <param name="answered">The position of the work the first fact answers, when it answers current work.</param>
    private Handled<WfCommand, WfEvent> Advance(
        WfRunning running,
        object firstFact,
        int? answered,
        DecisionContext<WfState, WfCommand, WfEvent> ctx,
        Handled<WfCommand, WfEvent> handled)
    {
        var input = (TInput)running.Input;
        var state = (TState)running.State;
        var entry = running.Entry;
        var enteredAt = running.EnteredAt;
        var settled = running.Settled;
        var held = running.HeldFacts;
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
                    (state, entry, enteredAt, settled, issued) = (next.State, entry + 1, ctx.UtcNow, [], false);
                    break;

                case AgainTransition:
                    Record(new WfRepeated());
                    (entry, settled, issued) = (entry + 1, [], false);
                    break;

                case FinishTransition<TOutcome> finish:
                    Record(new WfEnded(finish.Outcome, definition.Kind(finish.Outcome)));
                    finished = true;
                    break;

                case StayTransition:
                    Record(new WfKept());
                    if (position is { } kept) settled = settled.Add(kept);
                    break;

                case UnexpectedTransition:
                    Record(new WfUnexpected());
                    if (position is { } index) settled = settled.Add(index);
                    unexpected.Add((fact, state));
                    break;

                default:
                    throw new InvalidOperationException(
                        $"{definition.TypeName.Value}.Decide returned an uninitialized Transition for the fact {fact} " +
                        $"in the state {state}.");
            }
        }

        bool WorkInFlight() => issued && WorkOf(state, ctx, input, enteredAt).Length > settled.Length;

        Step(firstFact, answered, fromHeld: false);
        while (!finished && !held.IsEmpty && !WorkInFlight())
        {
            var next = held[0];
            held = held.RemoveAt(0);
            Step(next.Fact, position: null, fromHeld: true);
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

        // A state that was entered here has not had its work issued yet. Only the last one gets it.
        return finished || issued ? handled : Issue(handled, ctx, input, state, entry, enteredAt);
    }

    /// <summary>
    /// After a restart: every piece of the state's work that has no answer was interrupted. It is not issued
    /// again. The workflow receives the give-up fact for it, and the table decides.
    /// </summary>
    private Handled<WfCommand, WfEvent> Resume(WfState state, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        var handled = ctx.None();
        if (state is not WfRunning running) return handled;

        var work = WorkOf((TState)running.State, ctx, (TInput)running.Input, running.EnteredAt);
        for (var index = 0; index < work.Length; index++)
        {
            if (running.Settled.Contains(index)) continue;

            var effect = work[index].Effect;
            object answer = (WfCommand)new WfAnswer(running.Entry, index,
                definition.GaveUp(new WorkGaveUp(effect, GaveUpReason.Interrupted)));

            // Nothing runs: the notice comes back to the workflow through the path every answer takes.
            handled = handled.Add(new ExecuteEffect(new InterruptedWork(effect))
            {
                Resolve = _ => _ => Task.FromResult<object?>(answer),
            });
        }

        return handled;
    }

    /// <summary>Adds the work of <paramref name="state"/> to a decision, to run once its events are journaled.</summary>
    private Handled<WfCommand, WfEvent> Issue(
        Handled<WfCommand, WfEvent> handled,
        DecisionContext<WfState, WfCommand, WfEvent> ctx,
        TInput input,
        TState state,
        int entry,
        DateTimeOffset enteredAt)
    {
        var work = WorkOf(state, ctx, input, enteredAt);
        for (var index = 0; index < work.Length; index++)
            handled = handled.Add(Execute(work[index], entry, index));

        return handled;
    }

    private ImmutableArray<Work<TFact>> WorkOf(
        TState state, DecisionContext<WfState, WfCommand, WfEvent> ctx, TInput input, DateTimeOffset enteredAt)
    {
        var list = definition.Work(state, new WorkContext<TInput, TFact>(IdOf(ctx), input, enteredAt))
                   ?? throw new InvalidOperationException(
                       $"{definition.TypeName.Value}.Work returned null for the state {state}. " +
                       "Return ctx.Nothing() for a state that has no work.");
        return list.Items.IsDefault ? [] : list.Items;
    }

    private ExecuteEffect Execute(Work<TFact> work, int entry, int index)
    {
        if (work.Resolve is null)
            throw new InvalidOperationException("Work must be created through WorkContext.Execute.");

        return new ExecuteEffect(work.Effect)
        {
            // The handler returns one of the author's facts. The entity's command is the answer that carries it.
            Resolve = services =>
            {
                var invoke = work.Resolve(services);
                return async ct => (WfCommand)new WfAnswer(entry, index, await invoke(ct));
            },

            // A failure or a timeout is an answer too: the fact the definition names for work that gave up.
            Failed = failure => (WfCommand)new WfAnswer(entry, index, definition.GaveUp(new WorkGaveUp(
                work.Effect,
                failure is TimeoutException ? GaveUpReason.TimedOut : GaveUpReason.Failed,
                failure.Message))),
        };
    }

    private static WorkflowId IdOf(DecisionContext<WfState, WfCommand, WfEvent> ctx) => new(ctx.EntityId.Value);
}

internal static class WorkflowEvolve
{
    public static WfState Evolve(WfState state, WfEvent @event) =>
        (state, @event) switch
        {
            (WfNotStarted, WfStarted started) => new WfRunning(started.Input, started.First, 1, started.At, [], []),
            (WfRunning running, WfHeld held) =>
                running with { Held = running.HeldFacts.Add(new WfHeldFact(held.Fact, held.At)) },
            // A fact taken from the held list leaves it, then its step applies as for any fact.
            (WfRunning running, WfFactRecorded { FromHeld: true } recorded) =>
                Apply(running with { Held = running.HeldFacts.RemoveAt(0) }, recorded),
            (WfRunning running, WfFactRecorded recorded) => Apply(running, recorded),
            _ => throw new InvalidOperationException(
                $"The journal holds the workflow event {@event.Value} for a workflow in the state {state.Value}. " +
                "That event cannot follow that state, so the journal is not one this workflow wrote."),
        };

    private static WfState Apply(WfRunning running, WfFactRecorded recorded) =>
        recorded.Step switch
        {
            WfMoved moved => running with
            {
                State = moved.Next, Entry = running.Entry + 1, EnteredAt = recorded.At, Settled = [],
            },
            WfRepeated => running with { Entry = running.Entry + 1, Settled = [] },
            WfEnded ended => new WfFinished(running.Input, ended.Outcome, ended.Kind, recorded.At),
            WfKept or WfUnexpected => recorded.Answered is { } index
                ? running with { Settled = running.Settled.Add(index) }
                : running,
        };
}
