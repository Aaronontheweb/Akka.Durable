using Akka.Durable.Tests.Fixtures.Campaigns;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A workflow that finishes leaves no reminder behind: the decision that finishes it cancels the reminder, the
/// way a decision that leaves the workflow running stores one.
/// </summary>
public abstract class WorkflowWakeCancelSpecs(ITestOutputHelper output) : CampaignWorkflowKit(output)
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(300);
    private static readonly TimeSpan Long = TimeSpan.FromDays(15);

    // How long a short timer can take to come back: the timer, a reminder delivery, and a decision.
    private static readonly TimeSpan TimerFires = TimeSpan.FromSeconds(10);

    public enum Finisher { Fact, Timer, Cancel }

    [Theory]
    [InlineData(Finisher.Fact)]
    [InlineData(Finisher.Timer)]
    [InlineData(Finisher.Cancel)]
    public async Task After_a_finish_no_reminder_is_stored_for_the_workflow(Finisher finisher)
    {
        var id = $"c-{finisher}".ToLowerInvariant();
        var campaign = CampaignRef(id);

        object expected;
        switch (finisher)
        {
            case Finisher.Fact:
                // Waiting on a timer 15 days ahead: the reminder is stored for that date.
                await campaign.Start(new Invoice(id, DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 1), AskCancellation());
                var waiting = Assert.Single(await RemindersOf(id));
                Assert.True(waiting.DueTimeUtc > DateTimeOffset.UtcNow + TimeSpan.FromDays(14));

                Assert.IsType<Delivered>((await campaign.Notify(new Paid(), AskCancellation())).Value);
                expected = new PaidInFull();
                break;

            case Finisher.Timer:
                // Due at once; collections knows at once; the grace period's timer ends the campaign.
                await campaign.Start(new Invoice(id, DateTimeOffset.UtcNow, Long, Short, Reminders: 1), AskCancellation());
                expected = new SentToCollections();
                break;

            default:
                await campaign.Start(new Invoice(id, DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 1), AskCancellation());
                Assert.Single(await RemindersOf(id));

                Assert.IsType<Delivered>((await campaign.Cancel("customer asked", "user-17", AskCancellation())).Value);
                expected = new CampaignStopped("customer asked");
                break;
        }

        Assert.Equal(expected, (await campaign.WaitForOutcome(AskCancellation())).Value);
        Assert.IsType<WorkflowFinished<CampaignOutcome>>((await campaign.GetStateAsync(AskCancellation())).State.Value);

        // The decision that finished the workflow cancelled its reminder.
        Assert.Empty(await RemindersOf(id));
        Assert.True(ReminderStorage.Writes(id) >= 1);
    }

    [Theory]
    [InlineData(Finisher.Fact)]
    [InlineData(Finisher.Cancel)]
    public async Task A_finish_succeeds_when_the_reminder_store_refuses_the_cancel(Finisher finisher)
    {
        var id = $"x-{finisher}".ToLowerInvariant();
        var campaign = CampaignRef(id);
        await campaign.Start(new Invoice(id, DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 1), AskCancellation());
        Assert.Single(await RemindersOf(id));

        ReminderStorage.FailingCancels = true;
        var reply = finisher == Finisher.Fact
            ? await campaign.Notify(new Paid(), AskCancellation())
            : await campaign.Cancel("customer asked", "user-17", AskCancellation());

        // The caller gets its reply and the workflow ends up finished; the leftover wake is harmless.
        Assert.IsType<Delivered>(reply.Value);
        object expected = finisher == Finisher.Fact ? new PaidInFull() : new CampaignStopped("customer asked");
        Assert.Equal(expected, (await campaign.WaitForOutcome(AskCancellation())).Value);
        Assert.IsType<WorkflowFinished<CampaignOutcome>>((await campaign.GetStateAsync(AskCancellation())).State.Value);
        await AwaitAssertAsync(() => Assert.True(ReminderStorage.RefusedCancels >= 1), cancellationToken: TestCancellation);

        // The workflow stays finished.
        ReminderStorage.FailingCancels = false;
        Wake(id);
        Assert.IsType<WorkflowFinished<CampaignOutcome>>((await campaign.GetStateAsync(AskCancellation())).State.Value);
    }
}

public sealed class LocalWorkflowWakeCancelSpecs(ITestOutputHelper output) : WorkflowWakeCancelSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowWakeCancelSpecs(ITestOutputHelper output) : WorkflowWakeCancelSpecs(output)
{
    protected override bool Sharded => true;
}
