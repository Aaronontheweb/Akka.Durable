using Akka.Durable.Tests.Fixtures.Campaigns;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Timers: a state that waits for a point in time, and a state that does work and waits at once. One durable
/// reminder for each workflow wakes it when a timer is due.
/// </summary>
public abstract class WorkflowTimerSpecs(ITestOutputHelper output) : CampaignWorkflowKit(output)
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(300);
    private static readonly TimeSpan Long = TimeSpan.FromHours(1);

    // How long a short timer can take to come back: the timer, a reminder delivery, and a decision.
    private static readonly TimeSpan TimerFires = TimeSpan.FromSeconds(5);

    private static TaskCompletionSource<CampaignFact> Pending() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task A_timers_fact_is_decided_when_the_timer_is_due_and_the_workflow_moves_on()
    {
        var dueAt = DateTimeOffset.UtcNow + Short;
        var sentAt = new TaskCompletionSource<DateTimeOffset>(TaskCreationOptions.RunContinuationsAsynchronously);
        Services.Send = (_, _) =>
        {
            sentAt.TrySetResult(DateTimeOffset.UtcNow);
            return Task.FromResult<CampaignFact>(new ReminderSent());
        };

        await CampaignRef("t1").Start(new Invoice("t1", dueAt, Long, Long, Reminders: 2), AskCancellation());

        // The due date passed: the first reminder went out, and the campaign pauses before the next one.
        var running = await AwaitRunning("t1", running => Assert.Equal(new Pausing(1), StateOf(running)), TimerFires);
        Assert.True(await sentAt.Task.WaitAsync(TestCancellation) >= dueAt);
        Assert.Equal([new SendReminder("t1", 1)], Services.Issued);

        // The start, the due date, and the reminder sent.
        Assert.Equal(3, (await HostedState("t1")).SequenceNr);
        Assert.Equal(3, running.Entry);
    }

    [Fact]
    public async Task A_timer_given_as_a_date_and_a_timer_given_as_a_duration_both_fire()
    {
        var campaign = CampaignRef("t2");

        // Waiting is a date, Pausing a duration, and Escalating does work and waits a duration.
        await campaign.Start(new Invoice("t2", DateTimeOffset.UtcNow + Short, Short, Short, Reminders: 2), AskCancellation());

        Assert.Equal(new SentToCollections(), (await campaign.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal(
            [new SendReminder("t2", 1), new SendReminder("t2", 2), new NotifyCollections("t2")],
            Services.Issued);
    }

    [Fact]
    public async Task When_the_work_moves_the_workflow_on_first_the_timer_of_the_same_state_never_fires()
    {
        Services.Notify = (_, _) => Task.FromResult<CampaignFact>(new CollectionsDeclined());
        var campaign = CampaignRef("t3");

        // Due at once. Collections declines at once, so the campaign leaves Escalating before its grace period ends.
        await campaign.Start(new Invoice("t3", DateTimeOffset.UtcNow, Long, Short, Reminders: 1), AskCancellation());
        await AwaitRunning("t3", running => Assert.Equal(new Pausing(1), StateOf(running)), TimerFires);
        var before = await HostedState("t3");

        // The reminder now waits for the pause: nothing will wake the workflow for the grace period.
        var reminder = Assert.Single(await RemindersOf("t3"));
        Assert.True(reminder.DueTimeUtc > DateTimeOffset.UtcNow + Long / 2);

        // A wake that comes anyway, as late as the grace period's end, finds no such timer in Pausing.
        Wake("t3", DateTimeOffset.UtcNow + Short * 2);
        Assert.Equal(before, await HostedState("t3"));
        Assert.Equal([new SendReminder("t3", 1), new NotifyCollections("t3")], Services.Issued);
    }

    /// <summary>Runs a campaign to Escalating with collections' answer held open, until its grace period has passed.</summary>
    private async Task<StateSnapshot<WfState>> EscalatingPastItsGrace(string id, TaskCompletionSource<CampaignFact> notified)
    {
        Services.Notify = (_, ct) => notified.Task.WaitAsync(ct);
        await CampaignRef(id).Start(new Invoice(id, DateTimeOffset.UtcNow, Long, Short, Reminders: 1), AskCancellation());

        var escalating = await AwaitRunning(id, running => Assert.Equal(new Escalating(1), StateOf(running)), TimerFires);
        await AwaitAssertAsync(() => Assert.True(DateTimeOffset.UtcNow > escalating.EnteredAt + Short),
            TimerFires, cancellationToken: TestCancellation);

        // The grace period is due, but collections has not answered. A wake now journals nothing.
        var before = await HostedState(id);
        Wake(id);
        Wake(id, DateTimeOffset.UtcNow);
        Assert.Equal(before, await HostedState(id));
        Assert.Empty(Assert.IsType<WfRunning>(before.State.Value).Held);
        return before;
    }

    [Fact]
    public async Task A_timer_due_while_work_is_in_flight_is_decided_with_the_works_fact_when_the_workflow_stays()
    {
        var notified = Pending();
        var before = await EscalatingPastItsGrace("t4", notified);

        notified.SetResult(new CollectionsNotified());

        // Collections' fact keeps the state, so the workflow is at rest there and the grace period is decided:
        // two facts in one write.
        Assert.Equal(new SentToCollections(), (await CampaignRef("t4").WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal(before.SequenceNr + 2, (await HostedState("t4")).SequenceNr);
        var journal = await JournalOf("t4");
        Assert.Equal<object>((CampaignFact)new CollectionsNotified(), Assert.IsType<WfFactRecorded>(journal[^2]).Fact);
        var grace = Assert.IsType<WfFactRecorded>(journal[^1]);
        Assert.Equal<object>((CampaignFact)new GraceOver(), grace.Fact);
        Assert.Equal(1, grace.Answered);
        Assert.False(grace.FromHeld);
    }

    [Fact]
    public async Task A_timer_due_while_work_is_in_flight_never_fires_when_the_works_fact_moves_the_workflow_on()
    {
        var notified = Pending();
        var before = await EscalatingPastItsGrace("t7", notified);

        notified.SetResult(new CollectionsDeclined());

        // Collections declines: the campaign goes back to reminders. The grace period belonged to Escalating.
        var paused = await AwaitRunning("t7", running => Assert.Equal(new Pausing(1), StateOf(running)));
        Assert.Equal(before.SequenceNr + 1, (await HostedState("t7")).SequenceNr);
        Assert.Empty(paused.Settled);

        // A wake that comes now finds no grace period in Pausing.
        Wake("t7", DateTimeOffset.UtcNow);
        Assert.Equal(before.SequenceNr + 1, (await HostedState("t7")).SequenceNr);
    }

    [Fact]
    public async Task Work_that_polls_within_a_time_limit_stops_when_the_limit_passes_during_a_poll()
    {
        var checking = Pending();
        Services.Check = (_, ct) => checking.Task.WaitAsync(ct);
        var campaign = CampaignRef("t8");
        await campaign.Start(new Invoice("t8", DateTimeOffset.UtcNow + Long, Long, Short, Reminders: 2), AskCancellation());

        // The customer says they paid. The check for the payment stays open past the time limit.
        Assert.IsType<Delivered>((await campaign.Notify(new PaymentClaimed(), AskCancellation())).Value);
        var verifying = await AwaitRunning("t8", running =>
        {
            Assert.Equal(new Verifying(0), StateOf(running));
            Assert.Single(Services.Issued);
        });
        await AwaitAssertAsync(() => Assert.True(DateTimeOffset.UtcNow > verifying.EnteredAt + Short),
            TimerFires, cancellationToken: TestCancellation);
        var before = await HostedState("t8");

        // Not yet paid: the table asks again. The new entry's check is not issued yet, so the workflow is at rest,
        // and the time limit, which counts from the claim, is decided first.
        checking.SetResult(new NotYetPaid());

        await AwaitRunning("t8", running => Assert.Equal(new Pausing(0), StateOf(running)));
        Assert.Equal([new CheckPayment("t8")], Services.Issued);

        Assert.Equal(before.SequenceNr + 2, (await HostedState("t8")).SequenceNr);
        var journal = await JournalOf("t8");
        var notYet = Assert.IsType<WfFactRecorded>(journal[^2]);
        Assert.Equal<object>((CampaignFact)new NotYetPaid(), notYet.Fact);
        Assert.IsType<WfRepeated>(notYet.Step.Value);
        var overdue = Assert.IsType<WfFactRecorded>(journal[^1]);
        Assert.Equal<object>((CampaignFact)new CheckOverdue(), overdue.Fact);
        Assert.Equal<object>(new WfMoved((CampaignState)new Pausing(0)), overdue.Step.Value);
    }

    [Fact]
    public async Task A_fact_from_outside_ends_a_workflow_that_waits_on_a_timer_at_once()
    {
        var campaign = CampaignRef("t5");
        await campaign.Start(new Invoice("t5", DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 1), AskCancellation());

        // A wait holds nothing: the payment is decided, not held.
        Assert.IsType<Delivered>((await campaign.Notify(new Paid(), AskCancellation())).Value);

        Assert.Equal(new PaidInFull(), (await campaign.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Empty(Services.Issued);

        // A wake for a finished workflow is ignored.
        var finished = await HostedState("t5");
        Wake("t5");
        Assert.Equal(finished, await HostedState("t5"));
    }

    [Fact]
    public async Task A_timer_survives_a_restart_and_wakes_the_workflow_itself()
    {
        var dueAt = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(2);
        await CampaignRef("t6").Start(new Invoice("t6", dueAt, Long, Long, Reminders: 2), AskCancellation());

        await StopWorkflow("t6");
        Assert.True(DateTimeOffset.UtcNow < dueAt, "The workflow stopped after its timer was due; the test proves nothing.");

        // Nothing is sent to the workflow. Its reminder activates it, and the timer's fact is decided.
        await AwaitAssertAsync(
            () => Assert.Equal([new SendReminder("t6", 1)], Services.Issued),
            dueAt - DateTimeOffset.UtcNow + TimerFires, cancellationToken: TestCancellation);

        var running = await AwaitRunning("t6", running => Assert.Equal(new Pausing(1), StateOf(running)));
        Assert.True(running.EnteredAt >= dueAt);
    }
}

public sealed class LocalWorkflowTimerSpecs(ITestOutputHelper output) : WorkflowTimerSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowTimerSpecs(ITestOutputHelper output) : WorkflowTimerSpecs(output)
{
    protected override bool Sharded => true;
}
