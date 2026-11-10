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
[AkkaSerializable(Manifest = "durable-wf-running-v1")]
internal sealed record WfRunning(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object State,
    [property: AkkaField(2)] int Entry,
    [property: AkkaField(3)] DateTimeOffset EnteredAt,
    [property: AkkaField(4)] ImmutableArray<int> Settled) : IDurableEntityProtocol
{
    // ImmutableArray compares by reference; compare its contents so records with equal data are equal.
    public bool Equals(WfRunning? other) =>
        other is not null && Equals(Input, other.Input) && Equals(State, other.State) && Entry == other.Entry
        && EnteredAt == other.EnteredAt && Settled.SequenceEqual(other.Settled);

    public override int GetHashCode() => HashCode.Combine(Input, State, Entry, EnteredAt, Settled.Length);
}

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
[AkkaSerializable(Manifest = "durable-wf-fact-v1")]
internal sealed record WfFactRecorded(
    [property: AkkaField(0)] object Fact,
    [property: AkkaField(1)] int? Answered,
    [property: AkkaField(2)] DateTimeOffset At,
    [property: AkkaField(3)] WfStep Step) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-event-v1")]
internal readonly union WfEvent(WfStarted, WfFactRecorded) : IDurableEntityProtocol;

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

[AkkaSerializable(Manifest = "durable-wf-command-v1")]
internal readonly union WfCommand(WfStart, WfAnswer) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-start-accepted-v1", AllowEmpty = true)]
internal sealed record WfStartAccepted : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-start-refused-running-v1", AllowEmpty = true)]
internal sealed record WfStartRefusedRunning : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-start-refused-finished-v1")]
internal sealed record WfStartRefusedFinished([property: AkkaField(0)] object Outcome) : IDurableEntityProtocol;

[AkkaSerializable(Manifest = "durable-wf-start-reply-v1")]
internal readonly union WfStartReply(WfStartAccepted, WfStartRefusedRunning, WfStartRefusedFinished) : IDurableEntityProtocol;

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

    public EntityDefinition<WfState, WfCommand, WfEvent> Compile()
    {
        var options = definition.Options ?? WorkflowOptions.Default;

        return new EntityDefinition<WfState, WfCommand, WfEvent>(
            new EntityTypeName(definition.TypeName.Value),
            _ => new WfNotStarted(),
            Decide,
            WorkflowEvolve.Evolve,
            options.Hosting with { EffectTimeout = options.AttemptLimit })
        {
            Resume = Resume,
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

            (WfRunning running, WfAnswer answer) => Record(running, answer, ctx),

            // An answer for a workflow that has ended. A finished workflow is closed.
            (WfNotStarted or WfFinished, WfAnswer) => ctx.Ignore(),
        };

    private Handled<WfCommand, WfEvent> Begin(WfStart start, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        var input = (TInput)start.Input;
        var first = definition.Start(input);

        return Issue(
            ctx.Persist(new WfStarted(start.Input, first, ctx.UtcNow)).Reply(start, Accepted),
            ctx, input, first, entry: 1, enteredAt: ctx.UtcNow);
    }

    /// <summary>Decides one fact and journals it with what it led to.</summary>
    private Decision<WfCommand, WfEvent> Record(
        WfRunning running, WfAnswer answer, DecisionContext<WfState, WfCommand, WfEvent> ctx)
    {
        // Work is issued once for each entry, so a second answer to the same work is not expected.
        var current = answer.Entry == running.Entry;
        if (current && running.Settled.Contains(answer.Index)) return ctx.Ignore();

        var input = (TInput)running.Input;
        var state = (TState)running.State;
        var fact = (TFact)answer.Fact;

        var transition = definition.Decide(
            state, fact, new DecideContext<TInput, TState, TOutcome>(IdOf(ctx), input, ctx.UtcNow, running.EnteredAt));

        // A fact that answers work from an earlier entry is still decided, but settles nothing current.
        WfEvent Fact(WfStep step) => new WfFactRecorded(answer.Fact, current ? answer.Index : null, ctx.UtcNow, step);

        switch (transition)
        {
            case NextTransition<TState> next:
                return Issue(ctx.Persist(Fact(new WfMoved(next.State))),
                    ctx, input, next.State, running.Entry + 1, ctx.UtcNow);

            case AgainTransition:
                return Issue(ctx.Persist(Fact(new WfRepeated())),
                    ctx, input, state, running.Entry + 1, running.EnteredAt);

            case FinishTransition<TOutcome> finish:
                return ctx.Persist(Fact(new WfEnded(finish.Outcome, definition.Kind(finish.Outcome))));

            case StayTransition:
                return ctx.Persist(Fact(new WfKept()));

            case UnexpectedTransition:
                var (type, id) = (definition.TypeName.Value, ctx.EntityId.Value);
                return ctx.Persist(Fact(new WfUnexpected()))
                    .Run<ILoggerFactory>((loggers, _) =>
                    {
                        loggers.CreateLogger("Akka.Durable.Workflow").LogWarning(
                            "Workflow [{WorkflowType}/{WorkflowId}] received the fact {Fact} in the state {State}, " +
                            "which its table marks as unexpected. The state is unchanged.", type, id, fact, state);
                        return Task.CompletedTask;
                    });

            default:
                throw new InvalidOperationException(
                    $"{definition.TypeName.Value}.Decide returned an uninitialized Transition for the fact {fact} " +
                    $"in the state {state}.");
        }
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
            (WfNotStarted, WfStarted started) => new WfRunning(started.Input, started.First, 1, started.At, []),
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
