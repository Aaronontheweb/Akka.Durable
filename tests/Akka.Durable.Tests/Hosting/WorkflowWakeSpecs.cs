using Akka.Durable.Tests.Fixtures.Campaigns;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The workflow's reminder as a guarantee: it wakes a workflow whose process died in the middle of a call, and it
/// is stored before anything is journaled. When it cannot be stored, the workflow does nothing.
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
    public async Task When_the_reminder_cannot_be_stored_nothing_is_journaled_and_no_work_is_issued()
    {
        var campaign = CampaignRef("h1");
        await campaign.Start(new Invoice("h1", DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 2), AskCancellation());
        var before = await HostedState("h1");

        // Sending a reminder now would issue work that needs a wake. Its reminder cannot be stored.
        ReminderStorage.Failing = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => campaign.Notify(new SendNow(), AskCancellation(TimeSpan.FromSeconds(2))));
        ReminderStorage.Failing = false;

        Assert.Equal(before, await HostedState("h1"));
        Assert.Empty(Services.Issued);

        // Once the reminder can be stored, the same fact is decided and the work is issued.
        Assert.IsType<Delivered>((await campaign.Notify(new SendNow(), AskCancellation())).Value);
        await AwaitAssertAsync(() => Assert.Equal([new SendReminder("h1", 1)], Services.Issued),
            cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task When_the_journal_write_fails_after_the_reminder_was_stored_the_stale_wake_changes_nothing()
    {
        var dueAt = DateTimeOffset.UtcNow + Long;
        var campaign = CampaignRef("j1");
        await campaign.Start(new Invoice("j1", dueAt, Long, Long, Reminders: 1), AskCancellation());
        var before = await HostedState("j1");

        // The decision stores its reminder, then its journal write fails and the actor stops.
        await WithJournalWrite(write => write.Fail(), async () =>
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => campaign.Notify(new SendNow(), AskCancellation(TimeSpan.FromSeconds(2)))));

        Assert.Empty(Services.Issued);
        var stale = Assert.Single(await RemindersOf("j1"));
        Assert.True(stale.DueTimeUtc < dueAt);
        Assert.Equal(before, await HostedState("j1"));

        // The stale wake arrives. The workflow still waits for the due date: nothing is journaled, and the
        // reminder is stored again for the due date.
        await AwaitAssertAsync(async () =>
        {
            var reminder = Assert.Single(await RemindersOf("j1"));
            Assert.True(reminder.DueTimeUtc >= dueAt);
        }, BackstopArrives, cancellationToken: TestCancellation);

        Assert.Equal(before, await HostedState("j1"));
        Assert.Empty(Services.Issued);
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
