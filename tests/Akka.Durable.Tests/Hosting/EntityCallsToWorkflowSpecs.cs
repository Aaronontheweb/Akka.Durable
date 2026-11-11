using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Event;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Calls from an entity to a workflow, hosted. The entity's effect time-out is one second. A start is a recorded
/// effect that the library delivers until the workflow answers; a notify and a cancel are recorded and attempted once.
/// </summary>
public abstract class EntityCallsSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    // Each Quiet window proves that a call is not repeated.

    [Fact]
    public async Task A_start_and_a_notify_arrive_and_the_answer_of_the_notify_becomes_a_command()
    {
        await Issue("s1");
        await AwaitCampaign<WorkflowRunning<CampaignState>>("s1");

        await Bill("s1").Ask(new PayBill(), AskCancellation(FirstAsk));

        await AwaitAnswers("s1", "Delivered");
        await AwaitCampaign<WorkflowFinished<CampaignOutcome>>("s1", finished => Assert.IsType<PaidInFull>(finished.Outcome.Value));
    }

    [Fact]
    public async Task A_start_that_gets_no_answer_is_issued_again_until_it_is_answered()
    {
        Calls.Fail("start", upTo: 1, Break.Throw);

        await Issue("s2");

        await AwaitCampaign<WorkflowRunning<CampaignState>>("s2");
        await AwaitNoReminders("s2");
        Assert.Equal(2, Calls.Count("start"));
    }
}

public sealed class LocalEntityCallsSpecs(ITestOutputHelper output) : EntityCallsSpecs(output)
{
    [Fact]
    public async Task Two_starts_of_one_workflow_start_it_once_and_leave_nothing_pending()
    {
        await Issue("twice");
        await AwaitCampaign<WorkflowRunning<CampaignState>>("twice");
        await Issue("twice");

        await AwaitNoReminders("twice");
        await LetTimePass();

        Assert.Equal(1, WorkflowStarts);
        Assert.Equal(2, Calls.Count("start"));
        Assert.Empty(await PendingEffects<BillingCommand>("twice"));
        Assert.IsType<WorkflowRunning<CampaignState>>((await CampaignOf("twice")).Value);
    }

    [Fact]
    public async Task Two_starts_issued_at_the_same_time_start_the_workflow_once_and_leave_nothing_pending()
    {
        // Both decisions are made before either start is answered.
        Bill("both").Tell(new IssueBill(FarAway));
        Bill("both").Tell(new IssueBill(FarAway));

        await AwaitAttempts("start", 2);
        await AwaitNoReminders("both");
        await LetTimePass();

        Assert.Equal(1, WorkflowStarts);
        Assert.Equal(2, Calls.Count("start"));
        Assert.Empty(await PendingEffects<BillingCommand>("both"));
    }

    [Fact]
    public async Task A_start_of_a_finished_workflow_is_answered_as_finished_and_not_repeated()
    {
        // Someone else started the workflow and it finished.
        var campaign = Campaigns("done");
        await campaign.Start(Billing.InputOf(new EntityId("done"), FarAway), TestCancellation);
        await campaign.Cancel("first", "tester", TestCancellation);

        await Issue("done");

        await AwaitNoReminders("done");
        await LetTimePass();

        Assert.Equal(1, Calls.Count("start"));
        Assert.Equal(1, WorkflowStarts);
        var finished = Assert.IsType<WorkflowFinished<CampaignOutcome>>((await CampaignOf("done")).Value);
        Assert.Equal("first", Assert.IsType<CampaignStopped>(finished.Outcome.Value).Reason);
    }

    [Theory]
    [InlineData(Break.Throw)]
    [InlineData(Break.Hang)]
    [InlineData(Break.LoseAnswer)]
    public async Task A_start_with_no_answer_is_issued_again_after_a_wait_until_answered_and_then_stops(Break how)
    {
        var log = Errors();
        Calls.Fail("start", upTo: 2, how);

        await Issue("retry");

        // The wake is stored meanwhile.
        await AwaitAssertAsync(async () => Assert.NotEmpty(await RemindersOf("retry")), cancellationToken: TestCancellation);
        await AwaitCampaign<WorkflowRunning<CampaignState>>("retry");
        await AwaitNoReminders("retry");
        await LetTimePass();

        var attempts = Calls.Attempts.Where(a => a.Kind == "start").ToList();
        Assert.Equal(3, attempts.Count);
        // Not a loop: each attempt waits at least the policy's first wait of one second.
        Assert.All(attempts.Zip(attempts.Skip(1)), pair => Assert.True(pair.Item2.At - pair.Item1.At >= TimeSpan.FromMilliseconds(900),
            $"The start was issued again after {pair.Item2.At - pair.Item1.At}."));
        // A lost answer means the workflow had started; the repeat is answered "already running".
        Assert.Equal(1, WorkflowStarts);
        // Each attempt without an answer is logged as a warning.
        await AwaitAssertAsync(() => Assert.True(
            log.Count(e => e is Warning && LogCollector.Text(e).Contains("billing/retry")) >= 2), cancellationToken: TestCancellation);
    }

    [Theory]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task A_fact_or_a_cancel_for_a_workflow_that_was_never_started_is_answered_and_not_repeated(string kind)
    {
        await TellKind("never", kind);

        await AwaitAnswers("never", "WorkflowNotStarted");
        await AwaitNoReminders("never");
        await LetTimePass();
        Assert.Equal(1, Calls.Count(kind));
        Assert.IsType<WorkflowNotStarted>((await CampaignOf("never")).Value);
    }

    [Theory]
    [InlineData("notify", Break.Throw, "gave up: Failed")]
    [InlineData("notify", Break.Hang, "gave up: TimedOut")]
    [InlineData("cancel", Break.Throw, "gave up: Failed")]
    [InlineData("cancel", Break.Hang, "gave up: TimedOut")]
    public async Task A_notify_or_cancel_that_fails_or_times_out_is_given_up_and_not_sent_again(string kind, Break how, string expected)
    {
        await Issue("once");
        await AwaitCampaign<WorkflowRunning<CampaignState>>("once");
        Calls.Fail(kind, upTo: 1, how);

        await TellKind("once", kind);

        await AwaitAnswers("once", expected);
        await AwaitNoReminders("once");
        await LetTimePass();
        Assert.Equal(1, Calls.Count(kind));
        // It never reached the workflow, and nothing is kept to send it again.
        Assert.IsType<WorkflowRunning<CampaignState>>((await CampaignOf("once")).Value);
    }

    [Theory]
    [InlineData("notify", "PaidInFull")]
    [InlineData("cancel", "CampaignStopped")]
    public async Task A_notify_or_cancel_whose_answer_is_lost_is_given_up_and_not_sent_again(string kind, string outcome)
    {
        await Issue("lost");
        await AwaitCampaign<WorkflowRunning<CampaignState>>("lost");
        Calls.Fail(kind, upTo: 1, Break.LoseAnswer);

        await TellKind("lost", kind);

        await AwaitAnswers("lost", "gave up: Failed");
        await LetTimePass();
        Assert.Equal(1, Calls.Count(kind));
        var finished = Assert.IsType<WorkflowFinished<CampaignOutcome>>((await CampaignOf("lost")).Value);
        Assert.Equal(outcome, finished.Outcome.Value!.GetType().Name);
    }

    private async Task TellKind(string bill, string kind)
    {
        if (kind == "notify") await Bill(bill).Ask(new PayBill(), AskCancellation());
        else await Bill(bill).Ask(new StopCampaign("stop"), AskCancellation());
    }
}

public sealed class ShardedEntityCallsSpecs(ITestOutputHelper output) : EntityCallsSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>
/// An entity that is passivated when it is idle, with a long effect time-out. A call in flight keeps it up.
/// </summary>
public sealed class LocalEntityCallsInFlightSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(1);

    protected override EntityOptions Options =>
        EntityOptions.Default with { EffectTimeout = TimeSpan.FromSeconds(30), PassivateIdleAfter = IdleTimeout };

    [Theory]
    [InlineData("start")]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task An_idle_entity_is_not_passivated_while_a_call_is_in_flight_and_the_call_completes(string kind)
    {
        const string bill = "idle";
        if (kind != "start")
            await Campaigns(bill).Start(Billing.InputOf(new EntityId(bill), FarAway), TestCancellation);
        var gate = Calls.Hold(kind);

        switch (kind)
        {
            case "start": await Issue(bill); break;
            case "notify": await Bill(bill).Ask(new PayBill(), AskCancellation()); break;
            default: await Bill(bill).Ask(new StopCampaign("stop"), AskCancellation()); break;
        }

        await AwaitAttempts(kind, 1);
        var entity = await EntityActor(bill);
        Watch(entity);
        // Several idle time-outs: the host asks the entity to stop, and the entity stays up.
        await ExpectNoMsgAsync(IdleTimeout * 3, TestCancellation);
        gate.SetResult();

        // The stop was deferred, not dropped.
        await ExpectTerminatedAsync(entity, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

        // The answer was handled before the entity stopped: the new incarnation recovers it from the journal.
        if (kind == "start") await AwaitCampaign<WorkflowRunning<CampaignState>>(bill);
        else await AwaitAnswers(bill, "Delivered");

        await AwaitNoReminders(bill);
        Assert.Equal(1, Calls.Count(kind));
    }
}

/// <summary>An entity that is passivated between the attempts of a start still completes it.</summary>
public sealed class LocalEntityCallsPassivatedSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    protected override EntityOptions Options =>
        EntityOptions.Default with { EffectTimeout = Interval, PassivateIdleAfter = TimeSpan.FromMilliseconds(300) };

    [Fact]
    public async Task A_start_that_gets_no_answer_is_completed_by_the_wake_after_the_entity_was_passivated()
    {
        Calls.Fail("start", upTo: 1, Break.Throw);
        await Issue("passivated");
        var entity = await EntityActor("passivated");
        Watch(entity);
        await ExpectTerminatedAsync(entity, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

        // Nothing asks for the entity. The wake brings it back and it issues the start again.
        await AwaitCampaign<WorkflowRunning<CampaignState>>("passivated");
        await AwaitNoReminders("passivated");
        Assert.Equal(2, Calls.Count("start"));
    }
}

/// <summary>
/// A decision that calls a workflow its definition does not name. The workflow is hosted, so only the entity's
/// definition is at fault.
/// </summary>
public sealed class LocalUndeclaredWorkflowCallSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    protected override EntityDefinition<BillingState, BillingCommand, BillingEvent> Entity =>
        new(Billing.TypeName, _ => new BillingState(false, []), Billing.Decide, Billing.Evolve);

    [Theory]
    [InlineData("start")]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task A_call_to_an_undeclared_workflow_is_logged_and_dropped_and_the_entity_carries_on(string call)
    {
        var log = Errors();
        var bill = Bill("undeclared");

        switch (call)
        {
            case "start": await bill.Ask(new IssueBill(FarAway), AskCancellation()); break;
            case "notify": await bill.Ask(new PayBill(), AskCancellation()); break;
            default: await bill.Ask(new StopCampaign("stop"), AskCancellation()); break;
        }

        var before = await EntityActor("undeclared");
        await AwaitAssertAsync(() => Assert.NotEmpty(ErrorsAbout(log, "campaigns")), cancellationToken: TestCancellation);
        await LetTimePass();

        // One call, one error: nothing loops. Nothing reached the workflow, and nothing is kept for later.
        Assert.Single(ErrorsAbout(log, "campaigns"));
        Assert.Empty(Calls.Attempts);
        Assert.IsType<WorkflowNotStarted>((await CampaignOf("undeclared")).Value);
        Assert.Empty(await RemindersOf("undeclared"));
        Assert.Empty(await PendingEffects<BillingCommand>("undeclared"));

        // The entity was not restarted.
        Assert.Equal(before, await EntityActor("undeclared"));
        Assert.Empty((await bill.GetStateAsync(AskCancellation())).State.Answers);
    }
}

/// <summary>
/// The entity restarts while a call is in flight. The effect time-out is long enough that the restart comes first,
/// and the quiet window is longer than the time-out, so the call of the old incarnation has run out of time too.
/// </summary>
public sealed class LocalEntityCallsRestartSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    protected override EntityOptions Options => EntityOptions.Default with { EffectTimeout = TimeSpan.FromSeconds(4) };

    protected override TimeSpan Quiet => TimeSpan.FromSeconds(5);

    [Fact]
    public async Task A_start_in_flight_when_the_entity_restarts_is_completed_after_recovery()
    {
        Calls.Fail("start", upTo: 1, Break.Hang);
        await Issue("crash");
        await AwaitAttempts("start", 1);

        await Crash("crash");

        await AwaitCampaign<WorkflowRunning<CampaignState>>("crash");
        await AwaitNoReminders("crash");
        await LetTimePass();
        Assert.Equal(2, Calls.Count("start"));
        Assert.Equal(1, WorkflowStarts);
    }

    [Theory]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task A_notify_or_cancel_in_flight_when_the_entity_restarts_gives_up_as_interrupted_and_is_not_sent_again(string kind)
    {
        await Issue("crash-fact");
        await AwaitCampaign<WorkflowRunning<CampaignState>>("crash-fact");
        Calls.Fail(kind, upTo: 1, Break.Hang);
        await TellKind("crash-fact", kind);
        await AwaitAttempts(kind, 1);

        await Crash("crash-fact");

        await AwaitAnswers("crash-fact", "gave up: Interrupted");
        await LetTimePass();
        Assert.Equal(1, Calls.Count(kind));
        Assert.IsType<WorkflowRunning<CampaignState>>((await CampaignOf("crash-fact")).Value);
    }

    private async Task TellKind(string bill, string kind)
    {
        if (kind == "notify") await Bill(bill).Ask(new PayBill(), AskCancellation());
        else await Bill(bill).Ask(new StopCampaign("stop"), AskCancellation());
    }
}
