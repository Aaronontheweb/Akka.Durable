using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Calls from an entity to a workflow, hosted. The entity's effect time-out, and so the pace of a repeated
/// start, is one second.
/// </summary>
public abstract class EntityCallsSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    // Each Quiet window proves that a call is not repeated.

    [Fact]
    public async Task A_start_and_a_notify_arrive_and_their_answers_become_commands()
    {
        await Issue("s1", watched: true);
        await AwaitAnswers("s1", "Started");

        await Bill("s1").Ask(new PayBill(), AskCancellation(FirstAsk));

        await AwaitAnswers("s1", "Started", "Delivered");
        await AwaitCampaign<WorkflowFinished<CampaignOutcome>>("s1", finished => Assert.IsType<PaidInFull>(finished.Outcome.Value));
    }

    [Fact]
    public async Task A_start_that_gets_no_answer_is_issued_again_until_it_is_answered()
    {
        Calls.Fail("start", upTo: 1, Break.Throw);

        await Issue("s2", watched: false);

        await AwaitCampaign<WorkflowRunning<CampaignState>>("s2");
        await AwaitNoReminders("s2");
        Assert.Equal(2, Calls.Count("start"));
    }
}

public sealed class LocalEntityCallsSpecs(ITestOutputHelper output) : EntityCallsSpecs(output)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Two_starts_of_one_workflow_start_it_once_and_leave_no_pending_start(bool watched)
    {
        await Issue("twice", watched);
        await AwaitCampaign<WorkflowRunning<CampaignState>>("twice");
        await Issue("twice", watched);

        if (watched) await AwaitAnswersInAnyOrder("twice", "Started", "AlreadyRunning");
        await AwaitNoReminders("twice");
        await LetTimePass();

        Assert.Equal(1, WorkflowStarts);
        Assert.Equal(2, Calls.Count("start"));
        Assert.IsType<WorkflowRunning<CampaignState>>((await CampaignOf("twice")).Value);
    }

    [Fact]
    public async Task Two_starts_issued_at_the_same_time_start_the_workflow_once_and_leave_no_pending_start()
    {
        // Both decisions are made before either start is answered.
        Bill("both").Tell(new IssueBillWatched(FarAway));
        Bill("both").Tell(new IssueBillWatched(FarAway));

        await AwaitAnswersInAnyOrder("both", "Started", "AlreadyRunning");
        await AwaitNoReminders("both");
        await LetTimePass();

        Assert.Equal(1, WorkflowStarts);
        Assert.Equal(2, Calls.Count("start"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_start_of_a_finished_workflow_is_answered_as_finished_and_not_repeated(bool watched)
    {
        // Someone else started the workflow and it finished.
        var campaign = Campaigns("done");
        await campaign.Start(Billing.InputOf(new EntityId("done"), FarAway), TestCancellation);
        await campaign.Cancel("first", "tester", TestCancellation);

        await Issue("done", watched);

        if (watched) await AwaitAnswers("done", "AlreadyFinished`1");
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
    public async Task A_start_with_no_answer_is_issued_again_at_the_effect_time_out_until_answered_and_then_stops(Break how)
    {
        var log = Errors();
        Calls.Fail("start", upTo: 2, how);

        await Issue("retry", watched: false);

        // It is kept in a reminder meanwhile.
        await AwaitAssertAsync(async () => Assert.NotEmpty(await RemindersOf("retry")), cancellationToken: TestCancellation);
        await AwaitCampaign<WorkflowRunning<CampaignState>>("retry");
        await AwaitNoReminders("retry");
        await LetTimePass();

        var attempts = Calls.Attempts.Where(a => a.Kind == "start").ToList();
        Assert.Equal(3, attempts.Count);
        // One attempt per effect time-out, not a loop: the second attempt waits for the first one's time.
        Assert.All(attempts.Zip(attempts.Skip(1)), pair => Assert.True(pair.Item2.At - pair.Item1.At >= TimeSpan.FromMilliseconds(500),
            $"The start was issued again after {pair.Item2.At - pair.Item1.At}."));
        // A lost answer means the workflow had started; the repeat is answered "already running".
        Assert.Equal(1, WorkflowStarts);
        // Each attempt without an answer is logged as an error.
        await AwaitAssertAsync(() => Assert.True(ErrorsAbout(log, "retry").Count() >= 2), cancellationToken: TestCancellation);
    }

    [Theory]
    [InlineData(Break.Throw, "gave up: Failed")]
    [InlineData(Break.Hang, "gave up: TimedOut")]
    public async Task A_start_with_gaveUp_hands_control_to_the_entity_and_is_not_issued_again(Break how, string expected)
    {
        Calls.Fail("start", upTo: 1, how);

        await Issue("gaveup", watched: true);

        await AwaitAnswers("gaveup", expected);
        await AwaitNoReminders("gaveup");
        await LetTimePass();
        Assert.Equal(1, Calls.Count("start"));
        Assert.IsType<WorkflowNotStarted>((await CampaignOf("gaveup")).Value);

        // The entity may start again.
        await Issue("gaveup", watched: true);

        await AwaitAnswers("gaveup", expected, "Started");
        await AwaitCampaign<WorkflowRunning<CampaignState>>("gaveup");
    }

    // The give-up and the reminder are both due one EffectTimeout after the send. A delivery that was already on its
    // way when the give-up removed the reminder must not issue the start again. Found by review, not by a spec.
    [Fact]
    public async Task A_reminder_delivered_after_a_give_up_does_not_issue_the_start_again()
    {
        Calls.Fail("start", upTo: 1, Break.Throw);
        await Issue("stale", watched: true);
        await AwaitAnswers("stale", "gave up: Failed");

        var campaign = Billing.CampaignOf(new EntityId("stale"));
        var key = PendingStart.KeyOf(Campaign.Definition.TypeName, campaign);
        var stale = new Akka.Reminders.ReminderEnvelope(
            new Akka.Reminders.ReminderEntity(Billing.TypeName.Value, "stale"), new Akka.Reminders.ReminderKey(key),
            DateTimeOffset.UtcNow, Akka.Reminders.ReminderDeadline.Infinite,
            new PendingStart(Campaign.Definition.TypeName.Value, campaign.Value, Billing.InputOf(new EntityId("stale"), FarAway)));
        Host.Services.GetRequiredService<Akka.Hosting.IRequiredActor<EntityRegion<BillingCommand>>>().ActorRef
            .Tell(new Akka.Cluster.Sharding.ShardingEnvelope("stale", stale), ActorRefs.NoSender);

        await LetTimePass();
        Assert.Equal(1, Calls.Count("start"));
        Assert.IsType<WorkflowNotStarted>((await CampaignOf("stale")).Value);
    }

    [Theory]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task A_fact_or_a_cancel_for_a_workflow_that_was_never_started_is_answered_and_not_repeated(string kind)
    {
        await TellKind("never", kind);

        await AwaitAnswers("never", "WorkflowNotStarted");
        await LetTimePass();
        Assert.Equal(1, Calls.Count(kind));
        Assert.IsType<WorkflowNotStarted>((await CampaignOf("never")).Value);
        Assert.Empty(await RemindersOf("never"));
    }

    [Theory]
    [InlineData("notify", Break.Throw, "gave up: Failed")]
    [InlineData("notify", Break.Hang, "gave up: TimedOut")]
    [InlineData("cancel", Break.Throw, "gave up: Failed")]
    [InlineData("cancel", Break.Hang, "gave up: TimedOut")]
    public async Task A_notify_or_cancel_that_fails_or_times_out_is_given_up_and_not_sent_again(string kind, Break how, string expected)
    {
        await Issue("once", watched: false);
        await AwaitCampaign<WorkflowRunning<CampaignState>>("once");
        Calls.Fail(kind, upTo: 1, how);

        await TellKind("once", kind);

        await AwaitAnswers("once", expected);
        await LetTimePass();
        Assert.Equal(1, Calls.Count(kind));
        // It never reached the workflow, and nothing is kept to send it again.
        Assert.IsType<WorkflowRunning<CampaignState>>((await CampaignOf("once")).Value);
        Assert.Empty(await RemindersOf("once"));
    }

    [Theory]
    [InlineData("notify", "PaidInFull")]
    [InlineData("cancel", "CampaignStopped")]
    public async Task A_notify_or_cancel_whose_answer_is_lost_is_given_up_and_not_sent_again(string kind, string outcome)
    {
        await Issue("lost", watched: false);
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

/// <summary>The entity issues the start again from <c>Resume</c>, as the documentation recommends.</summary>
public sealed class EntityResumesStartSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    // Long, so that the restart comes before the time-out of the start that is in flight.
    protected override EntityOptions Options => EntityOptions.Default with { EffectTimeout = TimeSpan.FromSeconds(30) };

    protected override EntityDefinition<BillingState, BillingCommand, BillingEvent> Entity => Billing.Definition with
    {
        Resume = (state, ctx) => !state.Issued || state.Answers.Length > 0
            ? ctx.None()
            : ctx.None().Start(Campaign.Definition, Billing.CampaignOf(ctx.EntityId), Billing.InputOf(ctx.EntityId, FarAway),
                then: answer => new CampaignAnswered(answer.Value!.GetType().Name),
                gaveUp: failure => new CampaignUnreachable(failure.Reason)),
    };

    [Fact]
    public async Task The_answer_of_a_start_issued_from_Resume_after_a_restart_is_delivered()
    {
        Calls.Fail("start", upTo: 1, Break.Hang);
        await Issue("resume", watched: true);
        await AwaitAttempts("start", 1);

        await Crash("resume");

        await AwaitAnswers("resume", "Started");
        await AwaitNoReminders("resume");
        await LetTimePass();
        // The repeat that the reminder issues has no function to answer to, so the entity hears nothing more.
        await AwaitAnswers("resume", "Started");
        Assert.Equal(1, WorkflowStarts);
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
    [InlineData("start-watched")]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task An_idle_entity_is_not_passivated_while_a_call_is_in_flight_and_the_call_completes(string call)
    {
        var kind = call.Split('-')[0];
        const string bill = "idle";
        if (kind != "start")
            await Campaigns(bill).Start(Billing.InputOf(new EntityId(bill), FarAway), TestCancellation);
        var gate = Calls.Hold(kind);

        switch (call)
        {
            case "start": await Issue(bill, watched: false); break;
            case "start-watched": await Issue(bill, watched: true); break;
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
        switch (call)
        {
            case "start": await AwaitCampaign<WorkflowRunning<CampaignState>>(bill); break;
            case "start-watched": await AwaitAnswers(bill, "Started"); break;
            default: await AwaitAnswers(bill, "Delivered"); break;
        }

        if (call == "start") await AwaitNoReminders(bill);
    }
}

/// <summary>An entity that is passivated between the attempts of a start still completes it.</summary>
public sealed class LocalEntityCallsPassivatedSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    protected override EntityOptions Options =>
        EntityOptions.Default with { EffectTimeout = Interval, PassivateIdleAfter = TimeSpan.FromMilliseconds(300) };

    [Fact]
    public async Task A_start_that_gets_no_answer_is_completed_by_the_reminder_after_the_entity_was_passivated()
    {
        Calls.Fail("start", upTo: 1, Break.Throw);
        await Issue("passivated", watched: false);
        var entity = await EntityActor("passivated");
        Watch(entity);
        await ExpectTerminatedAsync(entity, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

        // Nothing asks for the entity. The reminder wakes it and it issues the start again.
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
    [InlineData("start", null)]
    [InlineData("start-watched", "gave up: Failed")]
    [InlineData("notify", "gave up: Failed")]
    [InlineData("cancel", "gave up: Failed")]
    public async Task A_call_to_an_undeclared_workflow_is_logged_and_given_up_and_the_entity_carries_on(string call, string? answer)
    {
        var log = Errors();
        var bill = Bill("undeclared");

        switch (call)
        {
            case "start": await bill.Ask(new IssueBill(FarAway), AskCancellation()); break;
            case "start-watched": await bill.Ask(new IssueBillWatched(FarAway), AskCancellation()); break;
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

        // The entity was not restarted, and its give-up command was handled.
        Assert.Equal(before, await EntityActor("undeclared"));
        var state = (await bill.GetStateAsync(AskCancellation())).State;
        Assert.Equal(answer is null ? [] : new[] { answer }, state.Answers);
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
        await Issue("crash", watched: false);
        await AwaitAttempts("start", 1);

        await Crash("crash");

        await AwaitCampaign<WorkflowRunning<CampaignState>>("crash");
        await AwaitNoReminders("crash");
        await LetTimePass();
        Assert.Equal(2, Calls.Count("start"));
        Assert.Equal(1, WorkflowStarts);
    }

    [Fact]
    public async Task The_answer_of_a_start_that_was_in_flight_when_the_entity_restarted_is_not_delivered()
    {
        // Documented: then and gaveUp belong to the incarnation that issued the start.
        Calls.Fail("start", upTo: 1, Break.Hang);
        await Issue("crash-then", watched: true);
        await AwaitAttempts("start", 1);

        await Crash("crash-then");

        await AwaitCampaign<WorkflowRunning<CampaignState>>("crash-then");
        await AwaitNoReminders("crash-then");
        await LetTimePass();
        Assert.Empty((await Bill("crash-then").GetStateAsync(AskCancellation())).State.Answers);
    }

    [Theory]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task A_notify_or_cancel_in_flight_when_the_entity_restarts_is_not_sent_again(string kind)
    {
        await Issue("crash-fact", watched: false);
        await AwaitCampaign<WorkflowRunning<CampaignState>>("crash-fact");
        Calls.Fail(kind, upTo: 1, Break.Hang);
        await TellKind("crash-fact", kind);
        await AwaitAttempts(kind, 1);

        await Crash("crash-fact");
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
