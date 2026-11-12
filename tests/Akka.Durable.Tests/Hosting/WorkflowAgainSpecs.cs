using Akka.Durable.Tests.Fixtures.Campaigns;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The campaign with a retry policy on the handler that tells collections, and with <c>Again(after)</c> where a
/// claimed payment is not there yet. Virtual time as in <see cref="WorkflowRetryKit"/>: dates are read from the
/// workflow's reminder, and its delivery is a wake that a spec sends.
/// </summary>
public abstract class CampaignRetryKit(ITestOutputHelper output) : CampaignWorkflowKit(output)
{
    protected static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);
    protected static readonly TimeSpan TimerFires = TimeSpan.FromSeconds(10);

    /// <summary>Null: no policy.</summary>
    protected virtual RetryPolicy? NotifyPolicy => null;

    /// <summary>When set, a check that finds no payment yet asks again after this delay.</summary>
    protected virtual TimeSpan? AgainAfter => null;

    protected override WorkflowDefinition<Invoice, CampaignState, CampaignFact, CampaignOutcome> Definition =>
        Campaign.Definition with
        {
            Decide = (state, fact, ctx) => AgainAfter is { } after && (state, fact) is (Verifying, NotYetPaid)
                ? ctx.Again(after)
                : Campaign.Decide(state, fact, ctx),
        };

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) => services
        .AddSingleton<CampaignServices>()
        .AddEffectHandler<SendReminder, CampaignFact, SendReminderHandler>()
        .AddEffectHandler<NotifyCollections, CampaignFact, NotifyCollectionsHandler>(NotifyPolicy)
        .AddEffectHandler<CheckPayment, CampaignFact, CheckPaymentHandler>();

    protected int Count<TEffect>() => Services.Issued.OfType<TEffect>().Count();

    /// <summary>
    /// Delivers the workflow's stored reminder as Akka.Reminders would, to a workflow that may be stopped. With
    /// <paramref name="asIfStoredFor"/> the delivery comes before the reminder is due.
    /// </summary>
    protected async Task DeliverReminder(string id, DateTimeOffset? asIfStoredFor = null) =>
        StoredReminders.Deliver(Sys, Assert.Single(await RemindersOf(id)), asIfStoredFor);

    protected Task AwaitCount<TEffect>(int count) => AwaitAssertAsync(
        () => Assert.Equal(count, Count<TEffect>()), TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

    /// <summary>
    /// Waits until the workflow's one reminder is its wake for <paramref name="date"/>: it carries that date exactly,
    /// and it is due no earlier.
    /// </summary>
    protected async Task AwaitWakeFor(string id, DateTimeOffset date)
    {
        await AwaitAssertAsync(async () => Assert.Equal(date, Assert.Single(await RemindersOf(id)).StoredFor()),
            TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        Assert.Single(await RemindersOf(id)).IsWakeFor(date);
    }

    /// <summary>
    /// Waits until the workflow has journaled a delayed repeat, and returns its date. The date is the delay after the
    /// decision that asked for it, which is also when the repeated entry was issued.
    /// </summary>
    private protected async Task<DateTimeOffset> AwaitRepeat(string id, TimeSpan after)
    {
        var repeated = await AwaitRunning(id, running => Assert.NotNull(running.Delayed));
        Assert.Equal(repeated.IssuedAt + after, repeated.Delayed);
        return repeated.Delayed!.Value;
    }

    /// <summary>A campaign that has sent its one reminder and is in <see cref="Escalating"/>, with the grace period given.</summary>
    private protected async Task<WfRunning> EscalatingWith(string id, TimeSpan grace)
    {
        await CampaignRef(id).Start(new Invoice(id, DateTimeOffset.UtcNow, Minute, grace, Reminders: 1), AskCancellation());
        return await AwaitRunning(id, running => Assert.Equal(new Escalating(1), StateOf(running)), TimerFires);
    }

    /// <summary>A campaign whose customer said they paid: in <see cref="Verifying"/>, with the grace period given.</summary>
    private protected async Task<WfRunning> VerifyingWith(string id, TimeSpan grace)
    {
        var campaign = CampaignRef(id);
        await campaign.Start(new Invoice(id, DateTimeOffset.UtcNow + TimeSpan.FromDays(1), TimeSpan.FromHours(10), grace, Reminders: 2), AskCancellation());
        Assert.IsType<Delivered>((await campaign.Notify(new PaymentClaimed(), AskCancellation())).Value);
        return await AwaitRunning(id, running => Assert.Equal(new Verifying(0), StateOf(running)));
    }
}

/// <summary>Work with a policy next to the state's timer: Backoff(10 min, 1 h, 3 attempts) on the call to collections.</summary>
public sealed class WorkflowRetryTimerSpecs(ITestOutputHelper output) : CampaignRetryKit(output)
{
    protected override RetryPolicy? NotifyPolicy => Retry.Backoff(10 * Minute, 60 * Minute, 3);

    [Fact]
    public async Task A_timer_due_while_the_work_waits_for_its_next_attempt_fires_and_ends_the_retries()
    {
        Services.Notify = (_, _) => throw new InvalidOperationException("collections is down");
        var entered = await EscalatingWith("m1", grace: 5 * Minute);

        // The reminder is due at the earlier of the pending retry (10 minutes) and the grace period (5 minutes).
        var waiting = await AwaitRunning("m1", running => Assert.NotNull(WorkAttempts.Find(running.Attempts, 0)?.NextAt));
        Assert.True(WorkAttempts.Find(waiting.Attempts, 0)!.NextAt >= entered.EnteredAt + 10 * Minute, "The retry is due less than 10 minutes after the work was issued.");
        var due = entered.EnteredAt + 5 * Minute;
        await AwaitWakeFor("m1", due);

        // Nothing is in flight, so the timer is decided at once, with no answer to wait for.
        Wake("m1", due);

        Assert.Equal(new SentToCollections(), (await CampaignRef("m1").WaitForOutcome(AskCancellation())).Value);
        Assert.Equal(1, Count<NotifyCollections>());

        // The finished workflow has no reminder, so the retry that was pending cannot wake it.
        await AwaitAssertAsync(async () => Assert.Empty(await RemindersOf("m1")), cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task A_pending_retry_due_before_the_timer_runs_and_leaves_the_timer_where_it_was()
    {
        var attempts = 0;
        Services.Notify = (_, _) => ++attempts == 1
            ? throw new InvalidOperationException("collections is down")
            : Task.FromResult<CampaignFact>(new CollectionsNotified());
        var entered = await EscalatingWith("m2", grace: 30 * Minute);
        await AwaitCount<NotifyCollections>(1);

        // The earlier date wins: the retry in 10 minutes, not the grace period in 30.
        var waiting = await AwaitRunning("m2", running => Assert.NotNull(WorkAttempts.Find(running.Attempts, 0)?.NextAt));
        var due = WorkAttempts.Find(waiting.Attempts, 0)!.NextAt!.Value;
        Assert.True(due >= entered.EnteredAt + 10 * Minute, "The retry is due less than 10 minutes after the work was issued.");
        Assert.True(due < entered.EnteredAt + 30 * Minute, "The retry is not due before the grace period ends.");
        await AwaitWakeFor("m2", due);

        Wake("m2", due);
        await AwaitCount<NotifyCollections>(2);

        // Answered: the workflow stays in Escalating, and its timer is still due 30 minutes after it entered the state.
        var grace = entered.EnteredAt + 30 * Minute;
        await AwaitWakeFor("m2", grace);
        Wake("m2", grace);
        Assert.Equal(new SentToCollections(), (await CampaignRef("m2").WaitForOutcome(AskCancellation())).Value);
        Assert.Equal(2, Count<NotifyCollections>());
    }
}

/// <summary><c>Again(after)</c> where a claimed payment is not there yet: the check is asked again after 10 minutes.</summary>
public sealed class WorkflowAgainAfterSpecs(ITestOutputHelper output) : CampaignRetryKit(output)
{
    protected override TimeSpan? AgainAfter => 10 * Minute;

    [Fact]
    public async Task Again_after_a_delay_issues_the_work_once_at_its_time_and_holds_nothing_until_then()
    {
        var starts = new List<DateTimeOffset>();
        var second = new TaskCompletionSource<CampaignFact>();
        Services.Check = (_, _) =>
        {
            starts.Add(DateTimeOffset.UtcNow);
            return starts.Count == 1 ? Task.FromResult<CampaignFact>(new NotYetPaid()) : second.Task;
        };
        await VerifyingWith("a1", grace: TimeSpan.FromHours(1));

        // The first check found nothing: the second is due 10 minutes later, and the grace period is an hour away.
        await AwaitCount<CheckPayment>(1);
        var due = await AwaitRepeat("a1", 10 * Minute);
        Assert.True(due >= starts[0] + 10 * Minute, "The repeat is due less than 10 minutes after the first check ran.");
        await AwaitWakeFor("a1", due);

        // Until then nothing is in flight: a fact from outside is decided, not held.
        Assert.IsType<Delivered>((await CampaignRef("a1").Notify(new PaymentClaimed(), AskCancellation())).Value);
        Wake("a1", due - TimeSpan.FromSeconds(1));
        var before = await HostedState("a1");
        Assert.Equal(1, Count<CheckPayment>());

        // At its time the work is issued once, however often the workflow is woken.
        Wake("a1", due);
        Wake("a1", due);
        await AwaitCount<CheckPayment>(2);
        Assert.Equal(before.SequenceNr + 1, (await HostedState("a1")).SequenceNr);
        Assert.Equal(2, Count<CheckPayment>());

        second.SetResult(new Paid());
        Assert.Equal(new PaidInFull(), (await CampaignRef("a1").WaitForOutcome(AskCancellation())).Value);
        Assert.Equal(2, Count<CheckPayment>());
    }

    [Fact]
    public async Task Again_after_a_delay_does_not_run_when_the_states_timer_due_earlier_moves_the_workflow_on()
    {
        Services.Check = (_, _) => Task.FromResult<CampaignFact>(new NotYetPaid());
        var entered = await VerifyingWith("a2", grace: 5 * Minute);
        await AwaitCount<CheckPayment>(1);

        // The timer is due before the repeat: the reminder is due then.
        Assert.True(await AwaitRepeat("a2", 10 * Minute) > entered.EnteredAt + 5 * Minute, "The repeat is due before the timer.");
        var due = entered.EnteredAt + 5 * Minute;
        await AwaitWakeFor("a2", due);

        Wake("a2", due);
        await AwaitRunning("a2", running => Assert.Equal(new Pausing(0), StateOf(running)));

        // The repeat belonged to Verifying: it does not come back in Pausing.
        Wake("a2", entered.EnteredAt + TimeSpan.FromHours(1));
        var after = await HostedState("a2");
        Assert.Equal(new Pausing(0), StateOf(Assert.IsType<WfRunning>(after.State.Value)));
        Assert.Equal(1, Count<CheckPayment>());
    }


    [Fact]
    public async Task A_stopped_workflow_with_a_delayed_Again_is_started_by_the_delivery_of_its_reminder_and_issues_the_work_when_it_is_due()
    {
        // The first check finds nothing, and the repeat is due 10 minutes later. The second finds the payment.
        var asked = new List<DateTimeOffset>();
        Services.Check = (_, _) =>
        {
            asked.Add(DateTimeOffset.UtcNow);
            return Task.FromResult<CampaignFact>(asked.Count == 1 ? new NotYetPaid() : new Paid());
        };
        await VerifyingWith("a3", grace: TimeSpan.FromHours(1));
        await AwaitCount<CheckPayment>(1);
        var due = await AwaitRepeat("a3", 10 * Minute);
        Assert.True(due >= asked[0] + 10 * Minute, "The repeat is due less than 10 minutes after the first check ran.");
        await AwaitWakeFor("a3", due);

        // Nothing is sent to the workflow: the reminder it stored is delivered to a workflow that is not running, first
        // before it is due. The workflow starts, takes the date from its journal and stores its reminder again for it.
        await StopWorkflow("a3");
        await DeliverReminder("a3", asIfStoredFor: due - TimeSpan.FromSeconds(1));
        var recovered = await AwaitRunning("a3", running => Assert.NotNull(running.Delayed));
        Assert.Equal(due, recovered.Delayed);
        await AwaitWakeFor("a3", due);
        Assert.Equal(1, Count<CheckPayment>());

        // Delivered when it is due, it issues the work.
        await DeliverReminder("a3");
        await AwaitCount<CheckPayment>(2);
        Assert.Equal(new PaidInFull(), (await CampaignRef("a3").WaitForOutcome(AskCancellation())).Value);
        Assert.Equal(2, Count<CheckPayment>());
    }

    [Fact]
    public async Task Again_after_a_delay_that_was_journaled_but_whose_reminder_could_not_be_stored_gets_its_reminder_at_the_next_recovery()
    {
        // The first check finds nothing; the decision is journaled, and then the reminder cannot be stored.
        var asked = new List<DateTimeOffset>();
        Services.Check = (_, _) =>
        {
            asked.Add(DateTimeOffset.UtcNow);
            if (asked.Count > 1) return Task.FromResult<CampaignFact>(new Paid());
            ReminderStorage.Failing = true;
            return Task.FromResult<CampaignFact>(new NotYetPaid());
        };
        await VerifyingWith("a5", grace: TimeSpan.FromHours(1));
        await AwaitAssertAsync(() => Assert.True(ReminderStorage.FailedWrites >= 1), cancellationToken: TestCancellation);
        ReminderStorage.Failing = false;

        // The next recovery stores the reminder again, for the date in the journal: nothing is sent to the workflow.
        var due = await AwaitRepeat("a5", 10 * Minute);
        Assert.True(due >= asked[0] + 10 * Minute, "The repeat is due less than 10 minutes after the first check ran.");
        await AwaitWakeFor("a5", due);
        Assert.Equal(1, Count<CheckPayment>());

        await DeliverReminder("a5");
        await AwaitCount<CheckPayment>(2);
        Assert.Equal(new PaidInFull(), (await CampaignRef("a5").WaitForOutcome(AskCancellation())).Value);
    }

    [Fact]
    public async Task Repeating_the_work_does_not_move_the_states_timer_which_counts_from_when_the_state_was_entered()
    {
        // A check that never finds the payment, asked again after 10 minutes, in a state whose time limit is 5.
        Services.Check = (_, _) => Task.FromResult<CampaignFact>(new NotYetPaid());
        var entered = await VerifyingWith("a4", grace: 5 * Minute);
        await AwaitCount<CheckPayment>(1);
        var timer = entered.EnteredAt + 5 * Minute;
        await AwaitRepeat("a4", 10 * Minute);
        await AwaitWakeFor("a4", timer);

        // The repeat moved nothing: a wake a moment before the limit leaves the workflow where it is, and a wake at the
        // limit counted from when the state was entered, not from the repeat, ends it.
        Wake("a4", timer - TimeSpan.FromMilliseconds(1));
        Assert.Equal(new Verifying(0), StateOf(Assert.IsType<WfRunning>((await HostedState("a4")).State.Value)));
        Wake("a4", timer);
        var pausing = await AwaitRunning("a4", running => Assert.Equal(new Pausing(0), StateOf(running)));
        Assert.True(pausing.EnteredAt > entered.EnteredAt);
        Assert.Equal(1, Count<CheckPayment>());
    }
}

/// <summary>The delay of <c>Again(after)</c> has to be positive.</summary>
public sealed class AgainDelaySpecs
{
    private static readonly DecideContext<string, string, string> Context =
        new(new WorkflowId("w"), "input", DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-3_600_000)]
    public void Again_with_a_delay_that_is_not_positive_throws(int milliseconds) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Context.Again(TimeSpan.FromMilliseconds(milliseconds)));

    [Fact]
    public void Again_with_a_positive_delay_is_a_transition_and_Again_without_one_is_unchanged()
    {
        Assert.IsType<AgainTransition>(Context.Again(TimeSpan.FromTicks(1)).Value);
        Assert.Equal(new AgainTransition(), Assert.IsType<AgainTransition>(Context.Again().Value));
    }
}
