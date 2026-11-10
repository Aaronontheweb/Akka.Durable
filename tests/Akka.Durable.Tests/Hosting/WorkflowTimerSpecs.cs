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

        // The wake for the grace period arrives. Pausing has no such timer, so the wake journals nothing and only
        // stores the reminder again, for the pause.
        await AwaitAssertAsync(async () =>
        {
            var reminder = Assert.Single(await RemindersOf("t3"));
            Assert.True(reminder.DueTimeUtc > DateTimeOffset.UtcNow + Long / 2);
        }, TimerFires, cancellationToken: TestCancellation);

        Assert.Equal(before, await HostedState("t3"));
        Assert.Equal([new SendReminder("t3", 1), new NotifyCollections("t3")], Services.Issued);
    }

    [Fact]
    public async Task A_timer_due_while_work_is_in_flight_is_held_once_and_decided_after_the_works_fact()
    {
        var notified = Pending();
        Services.Notify = (_, ct) => notified.Task.WaitAsync(ct);
        var campaign = CampaignRef("t4");

        await campaign.Start(new Invoice("t4", DateTimeOffset.UtcNow, Long, Short, Reminders: 1), AskCancellation());

        // The grace period ends while collections has not answered: the timer's fact is held, and remembers its timer.
        var running = await AwaitRunning("t4", running =>
        {
            Assert.Equal(new Escalating(1), StateOf(running));
            var held = Assert.Single(running.Held);
            Assert.Equal<object>((CampaignFact)new GraceOver(), held.Fact);
            Assert.Equal(running.Entry, held.AnswersEntry);
            Assert.Equal(1, held.AnswersPosition);
        }, TimerFires);
        var before = await HostedState("t4");

        // More wakes find the timer held. They journal nothing.
        Wake("t4");
        Wake("t4");
        Assert.Equal(before, await HostedState("t4"));
        Assert.Empty(running.Settled);

        notified.SetResult(new CollectionsNotified());

        // Collections' fact keeps the state; then the held grace period ends the campaign. Both in one write.
        Assert.Equal(new SentToCollections(), (await campaign.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal(before.SequenceNr + 2, (await HostedState("t4")).SequenceNr);
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
