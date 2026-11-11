using Akka.Durable.Tests.Fixtures.Checkouts;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Workflow work whose handler was registered with a retry policy: the schedule of the attempts, on virtual time.
/// The charge's policy is Backoff(1 min, 5 min, 6 attempts) and the effect time-out 300 ms.
/// </summary>
public sealed class WorkflowRetryScheduleSpecs(ITestOutputHelper output) : WorkflowRetryKit(output)
{
    [Theory]
    [InlineData("throws", 0)]
    [InlineData("faults", 0)]
    [InlineData("empty", 0)]
    [InlineData("times out", 300)]
    public async Task A_failed_attempt_is_attempted_again_at_the_time_it_failed_plus_the_policys_wait(string how, int runsMs)
    {
        ChargeFailsFirst(1, how);
        var checkout = CheckoutRef("s1");

        await checkout.Start(new PlaceOrder("s1", 100), AskCancellation());

        // The first attempt failed after runsMs: the next one is due a minute later, and not before.
        await AwaitCharges(1);
        var failedAt = await StartOf(1) + TimeSpan.FromMilliseconds(runsMs);
        var due = await AwaitRetryDue("s1", failedAt, Minute);
        var before = await HostedState("s1");

        Wake("s1", due - TimeSpan.FromSeconds(1));
        Assert.Equal(before, await HostedState("s1"));
        Assert.Single(Charges);

        Wake("s1", due);
        Assert.Equal(new OrderPlaced("charge-s1"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);

        // One piece of work: the same id on both attempts, and the attempt number counts.
        Assert.Equal([1, 2], Charges.Select(c => c.Attempt));
        Assert.Equal(Charges[0].Id, Charges[1].Id);
        Assert.StartsWith("checkouts/s1/", Charges[0].Id.Value, StringComparison.Ordinal);
        Assert.EndsWith(".0", Charges[0].Id.Value, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_wait_doubles_with_each_failure_and_stops_at_the_policys_maximum()
    {
        ChargeFailsFirst(5);
        var checkout = CheckoutRef("s2");
        await checkout.Start(new PlaceOrder("s2", 100), AskCancellation());

        // After attempts 1 to 5: 1, 2, 4 minutes, then 8 and 16 capped at 5.
        TimeSpan[] waits = [Minute, 2 * Minute, 4 * Minute, 5 * Minute, 5 * Minute];
        var previous = DateTimeOffset.MinValue;
        for (var n = 1; n <= 5; n++)
        {
            await AwaitCharges(n);
            var failedAt = await StartOf(n);
            var due = await AwaitReminderDue("s2", d => d > previous && d >= failedAt + waits[n - 1] - Fuzz);
            Assert.InRange(due, failedAt + waits[n - 1] - Fuzz, failedAt + waits[n - 1] + Slack);
            previous = due;
            Wake("s2", due);
        }

        Assert.Equal(new OrderPlaced("charge-s2"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal([1, 2, 3, 4, 5, 6], Charges.Select(c => c.Attempt));
        Assert.Single(Charges.Select(c => c.Id).Distinct());
    }

    [Fact]
    public async Task A_state_change_during_the_wait_drops_the_pending_retry_and_the_late_answer_of_the_old_attempt()
    {
        // Attempt 1 ignores its token and answers when the spec says: long after it timed out.
        var late = new TaskCompletionSource<CheckoutFact>();
        var undoGate = new TaskCompletionSource();
        Services.Charge = (_, _) => late.Task;
        Services.Undo = async (_, _) =>
        {
            await undoGate.Task;
            return new Undone();
        };
        var checkout = CheckoutRef("s3");
        await checkout.Start(new PlaceOrder("s3", 100), AskCancellation());
        var due = await AwaitReminderDue("s3", d => d > DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30));

        // The work waits for its next attempt, so it is not in flight: the cancel is decided at once.
        var cancelled = await checkout.Cancel("customer asked", "user-17", AskCancellation());
        Assert.IsType<Delivered>(cancelled.Value);
        await AwaitState("s3", new Undoing(new OrderCancelled("customer asked")));

        // The old retry never runs, and the late answer of the first attempt is ignored.
        Wake("s3", due + Minute);
        late.SetResult(new Charged("late"));
        undoGate.SetResult();

        Assert.Equal(new OrderCancelled("customer asked"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Single(Charges);
        Assert.Equal<object?>(
            [new Reserved("res-s3"), new CancelRequested("customer asked", "user-17"), new Undone()],
            [.. await FactsOf("s3")]);
    }


    [Fact]
    public async Task A_late_answer_of_an_earlier_attempt_is_dropped_while_a_later_attempt_is_in_flight()
    {
        // Both attempts ignore their token: each answers when the spec says.
        var first = new TaskCompletionSource<CheckoutFact>();
        var second = new TaskCompletionSource<CheckoutFact>();
        Services.Charge = (_, _) =>
        {
            ChargeStarts.Enqueue(DateTimeOffset.UtcNow);
            return ChargeStarts.Count == 1 ? first.Task : second.Task;
        };
        var checkout = CheckoutRef("s5");
        await checkout.Start(new PlaceOrder("s5", 100), AskCancellation());
        var due = await AwaitReminderDue("s5", d => d > DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30));
        Wake("s5", due);
        await AwaitCharges(2);

        // Attempt 1 answers after attempt 2 started: it is not the attempt in flight.
        first.SetResult(new Charged("late"));
        second.SetResult(new Charged("fresh"));

        Assert.Equal(new OrderPlaced("fresh"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal<object?>([new Reserved("res-s5"), new Charged("fresh")], [.. await FactsOf("s5")]);
    }

    [Fact]
    public async Task A_fact_held_while_an_attempt_was_in_flight_is_decided_when_that_attempt_fails_and_ends_the_retries()
    {
        // The attempt hangs until its time limit.
        Services.Charge = (_, ct) =>
        {
            ChargeStarts.Enqueue(DateTimeOffset.UtcNow);
            return Never(ct);
        };
        var checkout = CheckoutRef("s6");
        await checkout.Start(new PlaceOrder("s6", 100), AskCancellation());
        await AwaitCharges(1);

        Assert.IsType<Held>((await checkout.Cancel("customer asked", "user-17", AskCancellation())).Value);

        // The attempt runs out of time: the cancel is decided, and the work that would have been attempted again is gone.
        Assert.Equal(new OrderCancelled("customer asked"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Wake("s6", DateTimeOffset.UtcNow + 10 * Minute);
        Assert.Single(Charges);
        await AwaitAssertAsync(async () => Assert.Empty(await RemindersOf("s6")), cancellationToken: TestCancellation);
    }
    [Fact]
    public async Task A_fact_from_outside_between_two_attempts_is_decided_at_once_and_leaves_the_retry_in_place()
    {
        ChargeFailsFirst(1);
        var checkout = CheckoutRef("s4");
        await checkout.Start(new PlaceOrder("s4", 100), AskCancellation());
        await AwaitCharges(1);
        var due = await AwaitRetryDue("s4", await StartOf(1), Minute);

        // The table says Unexpected for this fact in Charging: the state stays, so the retry stays.
        Assert.IsType<Delivered>((await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);
        Assert.Single(Charges);

        Wake("s4", due);
        Assert.Equal(new OrderPlaced("charge-s4"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal([1, 2], Charges.Select(c => c.Attempt));
        Assert.Equal<object?>([new Reserved("res-s4"), new OperatorSaidRetry(), new Charged("charge-s4")], [.. await FactsOf("s4")]);
    }
}

/// <summary>A limited policy: Backoff(1 min, 5 min, 3 attempts). The effect time-out is 30 s.</summary>
public sealed class WorkflowRetryExhaustionSpecs(ITestOutputHelper output) : WorkflowRetryKit(output)
{
    protected override RetryPolicy? ChargePolicy => Retry.Backoff(Minute, 5 * Minute, 3);

    protected override TimeSpan EffectTimeout => TimeSpan.FromSeconds(30);

    [Fact]
    public async Task A_policy_that_runs_out_gives_up_once_as_retries_exhausted_with_the_attempts_and_the_last_error()
    {
        Services.Charge = (_, _) =>
        {
            ChargeStarts.Enqueue(DateTimeOffset.UtcNow);
            throw new InvalidOperationException($"refused {ChargeStarts.Count}");
        };
        await CheckoutRef("x1").Start(new PlaceOrder("x1", 100), AskCancellation());

        // Waits of 1 and 2 minutes, and no third wait.
        TimeSpan[] waits = [Minute, 2 * Minute];
        var previous = DateTimeOffset.MinValue;
        for (var n = 1; n <= 2; n++)
        {
            await AwaitCharges(n);
            var startedAt = await StartOf(n);
            var due = await AwaitReminderDue("x1", d => d > previous && d >= startedAt + waits[n - 1] - Fuzz);
            previous = due;
            Wake("x1", due);
        }

        var running = await SettledIn<NeedsAttention>("x1");

        Assert.Equal(
            new NeedsAttention(new Charging("res-x1"), "ChargeCard#3[refused 3] RetriesExhausted"),
            ((CheckoutState)running.State).Value);
        Assert.Equal([1, 2, 3], Charges.Select(c => c.Attempt));

        // A late wake changes nothing.
        var before = await HostedState("x1");
        Wake("x1", DateTimeOffset.UtcNow + 24 * Minute);
        Assert.Equal(before, await HostedState("x1"));
        Assert.Equal(3, Charges.Count);
    }

    [Fact]
    public async Task A_restart_that_cuts_off_the_last_attempt_gives_up_as_retries_exhausted_with_every_attempt_counted()
    {
        ChargeFailsFirst(1);
        await CheckoutRef("x2").Start(new PlaceOrder("x2", 100), AskCancellation());
        await AwaitCharges(1);
        var due = await AwaitRetryDue("x2", await StartOf(1), Minute);

        // Attempt 2, the second of three, is in flight when the workflow stops: nothing is left to wait for but
        // one more attempt, which is due a while after the cut-off.
        Services.Charge = (_, ct) => Never(ct);
        Wake("x2", due);
        await AwaitCharges(2);
        await RestartWorkflow("x2");

        var after = await AwaitReminderDue("x2", d => d > DateTimeOffset.UtcNow + Minute);
        Assert.InRange(after, due, due + TimeSpan.FromMinutes(10));
        Assert.Equal(2, Charges.Count);

        // Attempt 3 is the last: this one is cut off too, and the policy has run out.
        Services.Charge = (_, ct) => Never(ct);
        Wake("x2", after);
        await AwaitCharges(3);
        await RestartWorkflow("x2");

        var running = await SettledIn<NeedsAttention>("x2");
        Assert.Equal(
            new NeedsAttention(new Charging("res-x2"), "ChargeCard#3[] RetriesExhausted"),
            ((CheckoutState)running.State).Value);
        Assert.Equal([1, 2, 3], Charges.Select(c => c.Attempt));
    }
}

/// <summary>Work with a policy across restarts of the workflow, in both hosting modes. The effect time-out is 30 s.</summary>
public abstract class WorkflowRetryRestartSpecs(ITestOutputHelper output) : WorkflowRetryKit(output)
{
    protected override TimeSpan EffectTimeout => TimeSpan.FromSeconds(30);

    [Fact]
    public async Task The_attempt_count_and_the_next_attempt_date_survive_restarts()
    {
        ChargeFailsFirst(2);
        var checkout = CheckoutRef("r1");
        await checkout.Start(new PlaceOrder("r1", 100), AskCancellation());
        await AwaitCharges(1);
        var due1 = await AwaitRetryDue("r1", await StartOf(1), Minute);

        // The restart keeps the date: the workflow is not asked again early, and its reminder is the same.
        await RestartWorkflow("r1");
        var kept = Assert.Single(await RemindersOf("r1")).DueTimeUtc;
        Assert.InRange(kept, due1 - TimeSpan.FromSeconds(1), due1 + TimeSpan.FromSeconds(1));
        Assert.Single(Charges);

        // Attempt 2 is attempt 2, so its wait is two minutes, not one.
        Wake("r1", due1);
        await AwaitCharges(2);
        var due2 = await AwaitRetryDue("r1", await StartOf(2), 2 * Minute);
        await RestartWorkflow("r1");
        Assert.Single(Charges.Skip(1));

        Wake("r1", due2);
        Assert.Equal(new OrderPlaced("charge-r1"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal([1, 2, 3], Charges.Select(c => c.Attempt));
        Assert.Single(Charges.Select(c => c.Id).Distinct());
    }

    [Fact]
    public async Task An_attempt_that_had_no_answer_at_a_restart_counts_as_failed_and_is_repeated_after_its_time_limit_and_the_wait()
    {
        Services.Charge = (_, ct) =>
        {
            ChargeStarts.Enqueue(DateTimeOffset.UtcNow);
            return Charges.Count >= 2 ? Task.FromResult<CheckoutFact>(new Charged("c2")) : Never(ct);
        };
        var checkout = CheckoutRef("r2");
        await checkout.Start(new PlaceOrder("r2", 100), AskCancellation());
        await AwaitCharges(1);
        var started = await StartOf(1);

        await RestartWorkflow("r2");

        // Not issued at once: attempt 1 may have run up to its time limit, and the policy's minute follows.
        var due = await AwaitReminderDue("r2", d => d > DateTimeOffset.UtcNow + TimeSpan.FromSeconds(45));
        Assert.InRange(due, started + TimeSpan.FromSeconds(90) - Fuzz, started + TimeSpan.FromSeconds(90) + Slack);
        Assert.Single(Charges);

        Wake("r2", due - TimeSpan.FromSeconds(1));
        Assert.Single(Charges);
        Wake("r2", due);

        Assert.Equal(new OrderPlaced("c2"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal([1, 2], Charges.Select(c => c.Attempt));
        Assert.Equal(Charges[0].Id, Charges[1].Id);
    }
}

/// <summary>The outbox form: UntilAcknowledged(1 min, 5 min). The effect time-out is 300 ms.</summary>
public sealed class WorkflowRetryUntilAcknowledgedSpecs(ITestOutputHelper output) : WorkflowRetryKit(output)
{
    protected override RetryPolicy? ChargePolicy => Retry.UntilAcknowledged(Minute, 5 * Minute);

    [Fact]
    public async Task Work_is_attempted_until_acknowledged_across_restarts_and_is_answered_once()
    {
        // Seven failures, each followed by a restart.
        Services.Charge = (_, _) =>
        {
            ChargeStarts.Enqueue(DateTimeOffset.UtcNow);
            return ChargeStarts.Count < 8
                ? throw new InvalidOperationException("not yet")
                : Task.FromResult<CheckoutFact>(new Charged("acknowledged"));
        };
        var checkout = CheckoutRef("u1");
        await checkout.Start(new PlaceOrder("u1", 100), AskCancellation());

        // Waits after attempts 1 to 7: 1, 2, 4, 5, 5, 5, 5 minutes.
        TimeSpan[] waits = [Minute, 2 * Minute, 4 * Minute, 5 * Minute, 5 * Minute, 5 * Minute, 5 * Minute];
        for (var n = 1; n <= 7; n++)
        {
            await AwaitCharges(n);
            var failedAt = await StartOf(n);
            var due = await AwaitReminderDue("u1", d => d >= failedAt + waits[n - 1] - Fuzz);
            await RestartWorkflow("u1");
            Assert.Equal(n, Charges.Count);
            Wake("u1", due);
        }

        Assert.Equal(new OrderPlaced("acknowledged"), (await CheckoutRef("u1").WaitForOutcome(Poll, AskCancellation())).Value);

        Assert.Equal(Enumerable.Range(1, 8), Charges.Select(c => c.Attempt));
        Assert.Single(Charges.Select(c => c.Id).Distinct());

        // The table decided one answer for the work and gave up on nothing.
        Assert.Equal<object?>([new Reserved("res-u1"), new Charged("acknowledged")], [.. await FactsOf("u1")]);
        await AwaitAssertAsync(async () => Assert.Empty(await RemindersOf("u1")), cancellationToken: TestCancellation);
    }
}

/// <summary>Without a policy nothing changes: one attempt, then the give-up fact.</summary>
public sealed class WorkflowWithoutRetryPolicySpecs(ITestOutputHelper output) : WorkflowRetryKit(output)
{
    protected override RetryPolicy? ChargePolicy => null;

    [Fact]
    public async Task Work_whose_handler_has_no_policy_is_attempted_once_and_gives_up_as_failed()
    {
        ChargeFailsFirst(5);
        await CheckoutRef("n1").Start(new PlaceOrder("n1", 100), AskCancellation());

        var running = await SettledIn<NeedsAttention>("n1");

        Assert.Equal(
            new NeedsAttention(new Charging("res-n1"), "ChargeCard#1[boom] Failed"),
            ((CheckoutState)running.State).Value);
        Assert.Single(Charges);
    }
}

public sealed class LocalWorkflowRetryRestartSpecs(ITestOutputHelper output) : WorkflowRetryRestartSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowRetryRestartSpecs(ITestOutputHelper output) : WorkflowRetryRestartSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>Real time: Every(1.5 s, 3 attempts), with the reminder delivering the wake. The effect time-out is 30 s.</summary>
public sealed class WorkflowRetryRealTimeSpecs(ITestOutputHelper output) : WorkflowRetryKit(output)
{
    protected override RetryPolicy? ChargePolicy => Retry.Every(TimeSpan.FromMilliseconds(1500), attempts: 3);

    protected override TimeSpan EffectTimeout => TimeSpan.FromSeconds(30);

    [Fact]
    public async Task A_passivated_workflow_is_woken_by_its_reminder_for_the_next_attempt_which_is_not_early()
    {
        ChargeFailsFirst(1);
        var checkout = CheckoutRef("t1");
        await checkout.Start(new PlaceOrder("t1", 100), AskCancellation());
        var firstAt = await StartOf(1);
        await AwaitReminderDue("t1", d => d > firstAt + TimeSpan.FromSeconds(1));
        Assert.True(DateTimeOffset.UtcNow < firstAt + TimeSpan.FromSeconds(1.2), "The retry was almost due when the workflow stopped.");

        // Nothing is sent to the workflow: its reminder activates it.
        await StopWorkflow("t1");

        Assert.Equal(new OrderPlaced("charge-t1"), (await checkout.WaitForOutcome(Poll, AskCancellation(TimeSpan.FromSeconds(30)))).Value);
        var secondAt = await StartOf(2);
        Assert.True(secondAt >= firstAt + TimeSpan.FromMilliseconds(1500) - TimeSpan.FromMilliseconds(250),
            $"Attempt 2 started {secondAt - firstAt} after attempt 1.");
        Assert.Equal([1, 2], Charges.Select(c => c.Attempt));
    }
}
