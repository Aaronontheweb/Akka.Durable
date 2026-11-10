using System.Collections.Immutable;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Durable.TestKit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// The workflow engine's pure decisions. The checkout workflow is compiled to its entity definition, and
/// Decide, Evolve and Resume are driven with the Given / When / Then harness. No ActorSystem is involved.
/// </summary>
public class WorkflowEngineSpecs
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly PlaceOrder Order = new("o1", 4200);

    private static readonly EntityDefinition<WfState, WfCommand, WfEvent> Entity =
        new WorkflowEntity<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>(Checkout.Definition).Compile();

    private static DeciderScenario<WfState, WfCommand, WfEvent> Scenario() =>
        DeciderScenario.For(Entity).At(Now);

    // ---- Journal entries that lead to the state a test needs ----

    private static WfEvent Started() => new WfStarted(Order, (CheckoutState)new Reserving(), Now);

    private static WfEvent Recorded(CheckoutFact fact, int? answered, WfStep step, bool fromHeld = false) =>
        new WfFactRecorded(fact, answered, Now, step, fromHeld);

    private static WfEvent Held(CheckoutFact fact) => new WfHeld(fact);

    private static WfStep MovedTo(CheckoutState next) => new WfMoved(next);

    private static WfStep Kept() => new WfKept();

    private static WfStep Unexpected() => new WfUnexpected();

    private static WfStep Ended(CheckoutOutcome outcome) => new WfEnded(outcome);

    /// <summary>Reserving, then reserved: the workflow is in Charging, entry 2, with its work in flight.</summary>
    private static WfEvent[] InCharging() =>
        [Started(), Recorded(new Reserved("r1"), 0, MovedTo(new Charging("r1")))];

    /// <summary>The charge was declined: the workflow is in Undoing, entry 3, with its work in flight.</summary>
    private static WfEvent[] InUndoing() =>
    [
        .. InCharging(),
        Recorded(new Declined("no funds"), 0, MovedTo(new Undoing(new OrderFailed("no funds")))),
    ];

    /// <summary>The reservation got no answer and the ticket was opened: NeedsAttention, entry 2, with its work settled.</summary>
    private static WfEvent[] WaitingForAPerson() =>
    [
        Started(),
        Recorded(new WorkStuck("ReserveStock", GaveUpReason.Interrupted), 0,
            MovedTo(new NeedsAttention(new Reserving(), "ReserveStock Interrupted"))),
        Recorded(new TicketOpened(), 0, Kept()),
    ];

    /// <summary>The workflow ended at once: out of stock.</summary>
    private static WfEvent[] Finished() =>
    [
        Started(),
        Recorded(new OutOfStock(), 0, Ended(new OrderFailed("out of stock"))),
    ];

    private static WfCommand Answer(int entry, int index, CheckoutFact fact) => new WfAnswer(entry, index, fact);

    private static WfRunning RunningState(WfState state) => Assert.IsType<WfRunning>(state.Value);

    private static List<ExecuteEffect> IssuedWork(DeciderOutcome<WfState, WfCommand, WfEvent> outcome) =>
        [.. outcome.Effects.Select(e => e.Value).OfType<ExecuteEffect>()];

    // ---- Answers that are not for the current entry ----

    [Fact]
    public void A_late_answer_with_nothing_in_flight_is_decided_at_once_and_settles_nothing()
    {
        var outcome = Scenario()
            .Given(WaitingForAPerson())
            // The reserve work of entry 1 answers late. Entry 2 has no work in flight.
            .When(Answer(entry: 1, index: 0, new Charged("c1")))
            .ThenPersisted(Recorded(new Charged("c1"), answered: null, Unexpected()))
            .ThenState(state =>
            {
                var running = RunningState(state);
                Assert.Equal(2, running.Entry);
                Assert.Equal<int>([0], running.Settled);
                Assert.Empty(running.Held);
            });

        Assert.Empty(IssuedWork(outcome));
    }

    [Fact]
    public void A_late_answer_while_work_is_in_flight_is_held_and_decides_nothing()
    {
        var outcome = Scenario()
            .Given(Started())
            .When(Answer(entry: 0, index: 0, new Charged("c1")))
            .ThenPersisted(Held(new Charged("c1")))
            .ThenState(state =>
            {
                var running = RunningState(state);
                Assert.Equal<CheckoutState>(new Reserving(), (CheckoutState)running.State);
                Assert.Equal(1, running.Entry);
                Assert.Empty(running.Settled);
                Assert.Equal<object>((CheckoutFact)new Charged("c1"), Assert.Single(running.Held));
            });

        // The only effect is the reminder, for the deadline of the work that is still in flight.
        Assert.Empty(IssuedWork(outcome));
        Assert.IsType<ScheduleEffect>(Assert.Single(outcome.Effects).Value);
    }

    [Fact]
    public void A_second_answer_to_work_that_is_already_settled_is_ignored()
    {
        Scenario()
            .Given(WaitingForAPerson())
            .When(Answer(entry: 2, index: 0, new TicketOpened()))
            .ThenIgnored();
    }

    [Fact]
    public void A_delivered_value_that_is_not_one_of_the_workflows_facts_is_unhandled()
    {
        Scenario()
            .Given(Started())
            .When(new WfDeliver("not a checkout fact", WantsReply: true))
            .ThenUnhandled();
    }

    // ---- What a fact leads to ----

    [Fact]
    public void Held_facts_that_remain_when_the_workflow_finishes_are_not_decided()
    {
        var outcome = Scenario()
            .Given([
                .. InCharging(),
                Held(new CancelRequested("changed my mind", "user-17")),
                Held(new OperatorSaidRetry()),
            ])
            .When(Answer(entry: 2, index: 0, new Charged("c1")))
            .ThenPersisted(Recorded(
                new Charged("c1"), answered: 0, Ended(new OrderPlaced("c1"))))
            .ThenState(state =>
            {
                var finished = Assert.IsType<WfFinished>(state.Value);
                Assert.Equal<CheckoutOutcome>(new OrderPlaced("c1"), (CheckoutOutcome)finished.Outcome);
            });

        Assert.Empty(IssuedWork(outcome));
    }

    [Fact]
    public void A_state_the_workflow_passes_through_does_not_get_its_work_issued()
    {
        var outcome = Scenario()
            .Given(Started(), Held(new CancelRequested("changed my mind", "user-17")))
            .When(Answer(entry: 1, index: 0, new Reserved("r1")))
            .ThenPersisted(
                // The reservation moves the workflow to Charging, whose work is never issued ...
                Recorded(new Reserved("r1"), answered: 0, MovedTo(new Charging("r1"))),
                // ... because the held cancel is decided next and moves it on to Undoing.
                Recorded(new CancelRequested("changed my mind", "user-17"), answered: null,
                    MovedTo(new Undoing(new OrderCancelled("changed my mind"))), fromHeld: true))
            .ThenState(state =>
            {
                var running = RunningState(state);
                Assert.Equal<CheckoutState>(
                    new Undoing(new OrderCancelled("changed my mind")), (CheckoutState)running.State);
                Assert.Equal(3, running.Entry);
                Assert.Empty(running.Held);
            });

        // Only the work of the state the workflow ends in. ChargeCard is not among it.
        Assert.Equal<object>([new UndoOrder("o1")], IssuedWork(outcome).Select(e => e.Effect));
    }

    [Fact]
    public void Again_issues_the_states_work_for_a_new_entry()
    {
        var outcome = Scenario()
            .Given(InUndoing())
            .When(Answer(entry: 3, index: 0, new UndoRefused()))
            .ThenPersisted(Recorded(new UndoRefused(), answered: 0, new WfRepeated()))
            .ThenState(state =>
            {
                var running = RunningState(state);
                Assert.Equal<CheckoutState>(new Undoing(new OrderFailed("no funds")), (CheckoutState)running.State);
                Assert.Equal(4, running.Entry);
                Assert.Empty(running.Settled);
            });

        Assert.Equal<object>([new UndoOrder("o1")], IssuedWork(outcome).Select(e => e.Effect));
    }

    // ---- Cancel ----

    public static TheoryData<bool> WorkInFlight => [true, false];

    [Theory]
    [MemberData(nameof(WorkInFlight))]
    public async Task A_cancel_carries_who_asked_and_is_logged_whether_it_is_held_or_decided(bool workInFlight)
    {
        var cancel = new CancelRequested("changed my mind", "user-17");
        var outcome = Scenario()
            .Given(workInFlight ? [Started()] : WaitingForAPerson())
            .When(new WfCancel("changed my mind", By: "user-17"))
            .ThenPersisted(workInFlight ? Held(cancel) : Recorded(cancel, answered: null, Kept()));

        var loggers = new RecordingLoggers();
        using var services = new ServiceCollection().AddSingleton<ILoggerFactory>(loggers).BuildServiceProvider();
        var log = Assert.Single(outcome.Effects.Select(e => e.Value).OfType<RunEffect>());
        await log.ExecuteAsync(services, TestContext.Current.CancellationToken);

        var (level, message) = Assert.Single(loggers.Lines);
        Assert.Equal(LogLevel.Information, level);
        Assert.Equal("Workflow [checkouts/test-entity] was asked to cancel by user-17. Reason: changed my mind", message);
    }

    // ---- Recovery ----

    [Fact]
    public async Task Recovery_gives_up_on_work_that_has_no_answer()
    {
        var outcome = Scenario().Given(Started()).Resumed();

        var effect = Assert.Single(IssuedWork(outcome));
        Assert.Equal(new InterruptedWork(new ReserveStock("o1")), effect.Effect);

        // Nothing runs for the interrupted work. Its answer is the give-up fact, for entry 1, position 0.
        var answer = await effect.Resolve!(null!)(TestContext.Current.CancellationToken);
        Assert.Equal<object?>(Answer(1, 0, new WorkStuck("ReserveStock", GaveUpReason.Interrupted)), answer);
    }

    [Fact]
    public void Recovery_gives_up_once_for_each_piece_of_work_that_has_no_answer_and_on_nothing_that_is_settled()
    {
        var paired = new WorkflowEntity<string, string, string, string>(PairedWork.Definition).Compile();

        // Neither piece has an answer.
        var neither = DeciderScenario.For(paired).At(Now)
            .Given(new WfStarted("go", "pair", Now))
            .Resumed();
        Assert.Equal<object>(
            [new InterruptedWork(new PairedWork.Step("first")), new InterruptedWork(new PairedWork.Step("second"))],
            IssuedWork(neither).Select(e => e.Effect));

        // The first piece answered; only the second is given up on.
        var second = DeciderScenario.For(paired).At(Now)
            .Given(
                new WfStarted("go", "pair", Now),
                new WfFactRecorded("first done", 0, Now, new WfKept(), FromHeld: false))
            .Resumed();
        Assert.Equal<object>(
            [new InterruptedWork(new PairedWork.Step("second"))],
            IssuedWork(second).Select(e => e.Effect));

    }

    [Fact]
    public void Recovery_issues_nothing_for_a_running_state_whose_work_is_settled()
    {
        var outcome = Scenario().Given(WaitingForAPerson()).Resumed();

        Assert.Empty(outcome.Effects);
    }

    [Fact]
    public void Recovery_issues_nothing_for_a_workflow_that_is_not_started()
    {
        var outcome = Scenario().Resumed();

        Assert.Empty(outcome.Effects);
    }

    [Fact]
    public void Recovery_issues_nothing_for_a_workflow_that_has_finished()
    {
        var outcome = Scenario().Given(Finished()).Resumed();

        Assert.IsType<WfFinished>(Finished().Aggregate(Entity.InitialState(new EntityId("o1")), Entity.Evolve).Value);
        Assert.Empty(outcome.Effects);
    }

    // ---- Evolve ----

    [Fact]
    public void Evolve_rejects_an_event_that_cannot_follow_the_state()
    {
        var running = Entity.Evolve(Entity.InitialState(new EntityId("o1")), Started());

        Assert.IsType<WfRunning>(running.Value);
        Assert.Throws<InvalidOperationException>(() => Entity.Evolve(running, Started()));
    }

    [Fact]
    public void Evolve_rejects_a_fact_for_a_workflow_that_is_not_started()
    {
        var notStarted = Entity.InitialState(new EntityId("o1"));

        Assert.Throws<InvalidOperationException>(() => Entity.Evolve(notStarted, Held(new Reserved("r1"))));
    }

    [Fact]
    public void Evolve_rejects_any_event_for_a_workflow_that_has_finished()
    {
        var finished = Finished().Aggregate(Entity.InitialState(new EntityId("o1")), Entity.Evolve);

        Assert.IsType<WfFinished>(finished.Value);
        Assert.Throws<InvalidOperationException>(() => Entity.Evolve(finished, Held(new Reserved("r1"))));
    }
}

/// <summary>An <see cref="ILoggerFactory"/> whose loggers record every line.</summary>
internal sealed class RecordingLoggers : ILoggerFactory, ILogger
{
    public List<(LogLevel Level, string Message)> Lines { get; } = [];

    public ILogger CreateLogger(string categoryName) => this;

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Dispose()
    {
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        Lines.Add((logLevel, formatter(state, exception)));
}

/// <summary>A workflow whose first state has two pieces of work, for what a checkout state cannot show.</summary>
internal static class PairedWork
{
    public sealed record Step(string Name);

    public static readonly WorkflowDefinition<string, string, string, string> Definition = new(
        new WorkflowTypeName("paired"),
        Start: _ => "pair",
        Work: (_, ctx) => ctx.All(ctx.Execute(new Step("first")), ctx.Execute(new Step("second"))),
        Decide: (_, _, ctx) => ctx.Stay(),
        Cancel: _ => "cancel",
        GaveUp: failure => $"gave up on {failure.What}");
}
