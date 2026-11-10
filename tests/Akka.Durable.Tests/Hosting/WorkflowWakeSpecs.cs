using System.Collections.Concurrent;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Reminders;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The workflow's reminder as a guarantee. The journal write comes first; the reminder is stored next, before any
/// work is issued or any reply sent. Recovery stores it again. It wakes a workflow whose process died during a call.
/// </summary>
public abstract class WorkflowWakeSpecs(ITestOutputHelper output) : CampaignWorkflowKit(output)
{
    private static readonly TimeSpan AttemptLimit = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan Long = TimeSpan.FromHours(1);

    // The backstop wake is due this long after the work was issued. Allow for its delivery and the decision.
    private static readonly TimeSpan BackstopArrives =
        AttemptLimit + WorkflowWake.BackstopMargin + TimeSpan.FromSeconds(5);

    protected override WorkflowDefinition<Invoice, CampaignState, CampaignFact, CampaignOutcome> Definition =>
        Campaign.Definition with { Options = new WorkflowOptions { AttemptLimit = AttemptLimit } };

    [Fact]
    public async Task A_workflow_whose_process_died_during_a_call_is_woken_and_the_interrupted_work_gives_up()
    {
        var interrupted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Services.Send = async (_, ct) =>
        {
            // The call stays open until the workflow's actor stops and cancels it.
            await using var registration = ct.Register(() => interrupted.TrySetResult());
            await Task.Delay(Timeout.Infinite, ct);
            throw new InvalidOperationException("Unreachable: the delay only ends by cancellation.");
        };

        // Due at once, with a short pause after a reminder that may not have gone out.
        var invoice = new Invoice("g1", DateTimeOffset.UtcNow, Pause: TimeSpan.FromMilliseconds(300), Long, Reminders: 1);
        await CampaignRef("g1").Start(invoice, AskCancellation());
        var reminding = await AwaitRunning("g1", running =>
        {
            Assert.Equal(new Reminding(1), StateOf(running));
            Assert.Single(Services.Issued);
        });

        await StopWorkflow("g1");
        await interrupted.Task.WaitAsync(TestCancellation);
        Assert.True(DateTimeOffset.UtcNow < reminding.IssuedAt + AttemptLimit,
            "The call reached its attempt limit before the workflow stopped; the test proves nothing.");

        // Nothing is sent to the workflow. The reminder activates it, the interrupted send gives up, the table
        // pauses the campaign, and after the pause the reminder is sent again.
        await AwaitAssertAsync(
            () => Assert.Equal([new SendReminder("g1", 1), new SendReminder("g1", 1)], Services.Issued),
            BackstopArrives, cancellationToken: TestCancellation);

        // The start, the due date, the give-up, and the end of the pause.
        var again = await AwaitRunning("g1", running => Assert.Equal(new Reminding(1), StateOf(running)));
        Assert.Equal(4, (await HostedState("g1")).SequenceNr);
        Assert.True(again.EnteredAt >= reminding.IssuedAt + AttemptLimit);
    }

    [Fact]
    public async Task When_the_reminder_cannot_be_stored_no_work_is_issued()
    {
        var campaign = CampaignRef("h1");
        await campaign.Start(new Invoice("h1", DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 2), AskCancellation());

        // The fact is journaled, then the reminder for the new state cannot be stored. The send is not issued and
        // the caller gets no reply. The workflow restarts, and every recovery tries to store the reminder again.
        ReminderStorage.Failing = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => campaign.Notify(new SendNow(), AskCancellation(TimeSpan.FromSeconds(2))));
        await AwaitAssertAsync(() => Assert.True(ReminderStorage.FailedWrites >= 3), cancellationToken: TestCancellation);
        Assert.Empty(Services.Issued);
        ReminderStorage.Failing = false;

        // The next recovery stores the reminder. The send was never issued, and the workflow cannot tell that from
        // a send that was issued and lost: it gives up as interrupted, and the table pauses the campaign.
        await AwaitRunning("h1", running => Assert.Equal(new Pausing(0), StateOf(running)));
        Assert.Empty(Services.Issued);
        var reminder = Assert.Single(await RemindersOf("h1"));
        Assert.True(reminder.DueTimeUtc > DateTimeOffset.UtcNow + Long / 2);

        var journal = await JournalOf("h1");
        Assert.Equal(3, journal.Count);
        Assert.IsType<WfStarted>(journal[0]);
        var sendNow = Assert.IsType<WfFactRecorded>(journal[1]);
        Assert.Equal<object>((CampaignFact)new SendNow(), sendNow.Fact);
        Assert.Equal<object>(new WfMoved((CampaignState)new Reminding(1)), sendNow.Step.Value);
        var gaveUp = Assert.IsType<WfFactRecorded>(journal[2]);
        Assert.Equal<object>((CampaignFact)new WorkFailed(nameof(SendReminder), GaveUpReason.Interrupted), gaveUp.Fact);
        Assert.Equal<object>(new WfMoved((CampaignState)new Pausing(0)), gaveUp.Step.Value);
    }

    [Fact]
    public async Task When_the_journal_write_fails_the_reminder_is_untouched_and_no_work_is_issued()
    {
        var campaign = CampaignRef("j1");
        await campaign.Start(new Invoice("j1", DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 2), AskCancellation());
        var reminderBefore = Assert.Single(await RemindersOf("j1"));
        var stateBefore = await HostedState("j1");

        // The journal write fails and the actor stops. Nothing after the journal write runs.
        await WithJournalWrite(write => write.Fail(), async () =>
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => campaign.Notify(new SendNow(), AskCancellation(TimeSpan.FromSeconds(2)))));

        // Read before anything activates the workflow again: recovery stores the reminder anew.
        var reminderAfter = Assert.Single(await RemindersOf("j1"));
        Assert.Equal(reminderBefore.DueTimeUtc, reminderAfter.DueTimeUtc);
        Assert.Equal(reminderBefore.Message, reminderAfter.Message);
        Assert.Equal(1, ReminderStorage.Writes("j1"));
        Assert.Empty(Services.Issued);

        await AwaitStarted("j1");
        Assert.Equal(stateBefore, await HostedState("j1"));
        Assert.Empty(Services.Issued);
    }

    [Fact]
    public async Task Recovery_stores_the_reminder_again()
    {
        var dueAt = DateTimeOffset.UtcNow + Long;
        await CampaignRef("r1").Start(new Invoice("r1", dueAt, Long, Long, Reminders: 2), AskCancellation());
        Assert.Single(await RemindersOf("r1"));

        // As if the process died between the journal write and the reminder write.
        await ReminderStorage.CancelReminderAsync(
            new ReminderEntity(Definition.TypeName.Value, "r1"), new ReminderKey(WorkflowWake.Key), TestCancellation);
        Assert.Empty(await RemindersOf("r1"));

        await RestartWorkflow("r1");

        var reminder = Assert.Single(await RemindersOf("r1"));
        Assert.Equal(new ReminderKey(WorkflowWake.Key), reminder.Key);
        Assert.InRange(reminder.DueTimeUtc, dueAt, dueAt + TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task The_reminder_is_stored_before_work_is_issued_and_before_the_reply()
    {
        var campaign = CampaignRef("o1");
        await campaign.Start(new Invoice("o1", DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 2), AskCancellation());

        var order = new ConcurrentQueue<string>();
        ReminderStorage.Writing = reminder =>
        {
            if (reminder.Entity.EntityId == "o1") order.Enqueue("reminder");
        };
        Services.Send = (_, _) =>
        {
            order.Enqueue("work");
            return Task.FromResult<CampaignFact>(new ReminderSent());
        };

        Assert.IsType<Delivered>((await campaign.Notify(new SendNow(), AskCancellation())).Value);
        order.Enqueue("reply");
        await AwaitAssertAsync(() => Assert.Contains("work", order), cancellationToken: TestCancellation);

        // The first thing the decision did after its journal write was to store the reminder.
        var seen = order.ToList();
        Assert.Equal("reminder", seen[0]);
        Assert.Contains("reply", seen);
    }

    [Fact]
    public async Task A_reminder_delivered_early_wakes_the_workflow_once()
    {
        // Akka.Reminders fetches every reminder due within MaxSlippage. Other campaigns falling due just before
        // this one make the scheduler fetch several times inside that window, and each fetch can deliver it early.
        var slippage = ReminderSettings.MaxSlippage;
        var dueAt = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(2);
        for (var other = 1; other <= 10; other++)
            await CampaignRef($"e{other}").Start(
                new Invoice($"e{other}", dueAt - slippage * other / 11, Long, Long, Reminders: 2), AskCancellation());

        var sentAt = new TaskCompletionSource<DateTimeOffset>(TaskCreationOptions.RunContinuationsAsynchronously);
        Services.Send = (effect, _) =>
        {
            if (effect.Invoice == "e0") sentAt.TrySetResult(DateTimeOffset.UtcNow);
            return Task.FromResult<CampaignFact>(new ReminderSent());
        };
        await CampaignRef("e0").Start(new Invoice("e0", dueAt, Long, Long, Reminders: 2), AskCancellation());

        await AwaitRunning("e0", running => Assert.Equal(new Pausing(1), StateOf(running)),
            dueAt - DateTimeOffset.UtcNow + TimeSpan.FromSeconds(5));

        // One delivery decides the due date, early or not. The reminder was stored three times: for the due date,
        // for the send's deadline, and for the pause.
        Assert.Equal(1, ReminderStorage.Deliveries("e0"));
        Assert.Equal(3, ReminderStorage.Writes("e0"));

        // A timer fires at most MaxSlippage early, and only when the reminder system delivered it early.
        Assert.True(await sentAt.Task.WaitAsync(TestCancellation) >= dueAt - slippage);
    }
}

public sealed class LocalWorkflowWakeSpecs(ITestOutputHelper output) : WorkflowWakeSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowWakeSpecs(ITestOutputHelper output) : WorkflowWakeSpecs(output)
{
    protected override bool Sharded => true;
}
