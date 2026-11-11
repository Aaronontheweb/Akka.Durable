using System.Collections.Concurrent;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Retried workflow work across real restarts: new actor systems read the journal from SQLite. Time is virtual: the
/// wait between attempts is a date, which a spec reads from the workflow's reminder, and a spec sends the wake that the
/// reminder would send.
/// </summary>
public sealed class WorkflowRetrySqlSpecs
{
    private static CancellationToken Ct => SqlCheckoutRig.Ct;

    [Fact]
    public async Task The_attempt_count_and_the_next_attempt_date_survive_a_restart_and_the_work_is_answered_once()
    {
        // An attempt that hangs is cut off after 30 s; the wait between attempts is a minute.
        var timeout = TimeSpan.FromSeconds(30);
        using var rig = new SqlCheckoutRig { ChargePolicy = Retry.Every(SqlCheckoutRig.Minute, attempts: 4), EffectTimeout = timeout };

        // Attempt 1 fails, attempt 2 hangs, and the host stops with attempt 2 in flight.
        var started = new ConcurrentQueue<DateTimeOffset>();
        var attemptTwoStart = default(DateTimeOffset);
        var first = new CheckoutServices();
        first.Charge = (_, _) =>
        {
            started.Enqueue(DateTimeOffset.UtcNow);
            return started.Count == 1
                ? Task.FromException<CheckoutFact>(new InvalidOperationException("not yet"))
                : Task.Delay(Timeout.Infinite).ContinueWith<CheckoutFact>(_ => default);
        };
        using (var one = await rig.StartAsync(first))
        {
            await SqlCheckoutRig.Ref(one).Start(new PlaceOrder("w1", 100), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
            var failed = Assert.IsType<WfRunning>((await SqlCheckoutRig.AwaitState(one, r => WorkAttempts.Find(r.Attempts, 0) is { NextAt: not null })).State.Value);
            SqlCheckoutRig.Wake(one, Assert.IsType<WfAttempt>(Assert.Single(WorkAttempts.Of(failed.Attempts))).NextAt!.Value);
            await SqlCheckoutRig.Until(() => started.Count == 2, "attempt 2");

            // The start of attempt 2 as the workflow journaled it, before its handler ran.
            var inFlight = Assert.IsType<WfRunning>((await SqlCheckoutRig.AwaitState(one, r => WorkAttempts.Find(r.Attempts, 0) is { Attempt: 2, NextAt: null })).State.Value);
            attemptTwoStart = Assert.IsType<WfAttempt>(WorkAttempts.Find(inFlight.Attempts, 0)).StartedAt;
            await one.StopAsync(Ct);
        }

        // The journaled start is real: it comes before the handler ran.
        Assert.True(attemptTwoStart <= started.ElementAt(1), "Attempt 2 was journaled as started after its handler ran.");
        var attemptTwo = SqlCheckoutRig.Deliveries<ChargeCard>(first)[1];

        // The new system reads the attempt count and the policy from the journal. Attempt 2 counts as failed, and
        // attempt 3 is due after its time limit and the wait: its reminder says so, and it is not attempted before.
        var second = new CheckoutServices();
        using (var two = await rig.StartAsync(second))
        {
            var checkout = SqlCheckoutRig.Ref(two);
            var due = attemptTwoStart + timeout + SqlCheckoutRig.Minute;
            var running = Assert.IsType<WfRunning>((await SqlCheckoutRig.AwaitState(two, r => WorkAttempts.Find(r.Attempts, 0) is { NextAt: not null })).State.Value);
            var waiting = Assert.IsType<WfAttempt>(Assert.Single(WorkAttempts.Of(running.Attempts)));
            Assert.Equal(new WfAttempt(0, 2, attemptTwoStart, due), waiting);
            (await SqlCheckoutRig.AwaitReminder(two, r => r.StoredFor() == due)).IsWakeFor(due);
            SqlCheckoutRig.Wake(two, due - TimeSpan.FromSeconds(1));
            await SqlCheckoutRig.HostedState(two);
            Assert.Empty(second.Issued);

            SqlCheckoutRig.Wake(two, due);
            Assert.Equal(new OrderPlaced("charge-w1"), (await checkout.WaitForOutcome(TimeSpan.FromMilliseconds(20), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)))).Value);

            var attemptThree = Assert.Single(SqlCheckoutRig.Deliveries<ChargeCard>(second));
            Assert.Equal(3, attemptThree.Attempt);
            Assert.Equal(attemptTwo.Id, attemptThree.Id);
            await two.StopAsync(Ct);
        }

        // A third system finds the work answered: the workflow is finished and nothing is attempted.
        var third = new CheckoutServices();
        using var last = await rig.StartAsync(third);
        Assert.Equal(new OrderPlaced("charge-w1"), (await SqlCheckoutRig.Ref(last).WaitForOutcome(TimeSpan.FromMilliseconds(20), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)))).Value);
        Assert.Empty(third.Issued);
        await last.StopAsync(Ct);
    }
}
