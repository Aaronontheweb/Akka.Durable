using Akka.Durable.Tests.Fixtures.Checkouts;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A workflow snapshot that holds retried work, written to and read from a real snapshot store. The events below
/// the snapshot are deleted before the second system starts, so the workflow can only come from the snapshot.
/// </summary>
public sealed class WorkflowSnapshotSqlSpecs
{
    private static CancellationToken Ct => SqlCheckoutRig.Ct;

    /// <summary>Waits for the snapshot of the given sequence number, stops the host, and removes the events it covers.</summary>
    private static async Task StopAfterSnapshotAndDeleteEvents(SqlCheckoutRig rig, IHost host, long sequenceNr)
    {
        await SqlCheckoutRig.Until(() => rig.Rows("snapshot") == 1, "the snapshot");
        await host.StopAsync(Ct);
        Assert.Equal(sequenceNr, rig.Scalar("SELECT sequence_number FROM snapshot"));
        Assert.Equal(sequenceNr, rig.Scalar("SELECT COUNT(*) FROM journal"));
        Assert.Equal((int)sequenceNr, rig.Execute(
            "DELETE FROM journal WHERE persistence_id = @id AND sequence_number <= @n",
            ("@id", SqlCheckoutRig.PersistenceId), ("@n", sequenceNr)));
        Assert.Equal(0, rig.Rows("journal"));
    }

    [Fact]
    public async Task A_snapshot_with_an_attempt_waiting_for_its_next_try_restores_the_count_and_the_date_and_the_work_runs_once_at_its_time()
    {
        // Events: the start, the reservation, attempt 1 failed, attempt 2 started, attempt 2 failed. The snapshot is
        // taken with the fifth.
        using var rig = new SqlCheckoutRig { ChargePolicy = Retry.Backoff(SqlCheckoutRig.Minute, 5 * SqlCheckoutRig.Minute, 6), SnapshotEvery = 5 };
        var first = new CheckoutServices { Charge = (_, _) => throw new InvalidOperationException("not yet") };
        StateSnapshot<WfState> before;
        DateTimeOffset firstNext;
        DateTimeOffset secondNext;
        using (var one = await rig.StartAsync(first))
        {
            await SqlCheckoutRig.Ref(one).Start(new PlaceOrder("w1", 100), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
            var waiting = await SqlCheckoutRig.AwaitState(one, r => WorkAttempts.Find(r.Attempts, 0) is { Attempt: 1, NextAt: not null });
            firstNext = WorkAttempts.Find(((WfRunning)waiting.State.Value).Attempts, 0)!.NextAt!.Value;

            SqlCheckoutRig.Wake(one, firstNext);
            before = await SqlCheckoutRig.AwaitState(one, r => WorkAttempts.Find(r.Attempts, 0) is { Attempt: 2, NextAt: not null });
            secondNext = WorkAttempts.Find(((WfRunning)before.State.Value).Attempts, 0)!.NextAt!.Value;
            Assert.Equal(5, before.SequenceNr);
            await StopAfterSnapshotAndDeleteEvents(rig, one, 5);
        }

        // Backoff: the wait after attempt 2 is 2 minutes, counted from its failure, which came after its journaled start.
        var secondStart = WorkAttempts.Find(((WfRunning)before.State.Value).Attempts, 0)!.StartedAt;
        Assert.True(secondNext >= secondStart + 2 * SqlCheckoutRig.Minute, "The second wait is shorter than 2 minutes.");

        var second = new CheckoutServices();
        using var two = await rig.StartAsync(second);

        // The state is the snapshot's: the same position, the same attempt, the same date. Nothing was replayed.
        var after = await SqlCheckoutRig.HostedState(two);
        Assert.Equal(5, after.SequenceNr);
        var running = Assert.IsType<WfRunning>(after.State.Value);
        Assert.Equal(before.State.Value, running);
        var attempt = Assert.IsType<WfAttempt>(Assert.Single(WorkAttempts.Of(running.Attempts)));
        Assert.Equal(new WfAttempt(0, 2, attempt.StartedAt, secondNext), attempt);
        Assert.Equal("res-w1", ((Charging)((CheckoutState)running.State).Value!).Reservation);
        Assert.Null(running.Delayed);

        // The reminder is stored again for the date in the snapshot, and nothing runs before it.
        (await SqlCheckoutRig.AwaitReminder(two, r => r.StoredFor() == secondNext)).IsWakeFor(secondNext);
        SqlCheckoutRig.Wake(two, secondNext - TimeSpan.FromSeconds(1));
        Assert.Equal(5, (await SqlCheckoutRig.HostedState(two)).SequenceNr);
        Assert.Empty(second.Issued);

        // At its time attempt 3 runs, once, and is answered.
        SqlCheckoutRig.Wake(two, secondNext);
        var outcome = await SqlCheckoutRig.Ref(two).WaitForOutcome(SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
        Assert.Equal(new OrderPlaced("charge-w1"), outcome.Value);
        Assert.Equal([new ChargeCard("w1", 100)], second.Issued);
        var delivery = Assert.Single(SqlCheckoutRig.Deliveries<ChargeCard>(second));
        Assert.Equal(3, delivery.Attempt);
        Assert.Equal(SqlCheckoutRig.Deliveries<ChargeCard>(first)[0].Id, delivery.Id);
        await two.StopAsync(Ct);
    }

    [Fact]
    public async Task A_snapshot_with_a_delayed_Again_restores_the_date_and_the_work_is_issued_once_at_its_time()
    {
        // Events: the start, the reservation, the decline (into Undoing), the refused undo that asks again after a
        // delay. The snapshot is taken with the fourth.
        using var rig = new SqlCheckoutRig { AgainAfterRefusedUndo = true, SnapshotEvery = 4 };
        var first = new CheckoutServices
        {
            Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("card declined")),
            Undo = (_, _) => Task.FromResult<CheckoutFact>(new UndoRefused()),
        };
        StateSnapshot<WfState> before;
        DateTimeOffset repeatAt;
        using (var one = await rig.StartAsync(first))
        {
            await SqlCheckoutRig.Ref(one).Start(new PlaceOrder("w1", 100), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
            before = await SqlCheckoutRig.AwaitState(one, r => r.Delayed is not null);
            repeatAt = ((WfRunning)before.State.Value).Delayed!.Value;
            // The repeat is due its delay after the decision that asked for it, which is when the new entry was issued.
            Assert.Equal(((WfRunning)before.State.Value).IssuedAt + SqlCheckoutRig.UndoDelay, repeatAt);
            Assert.Equal(4, before.SequenceNr);
            await StopAfterSnapshotAndDeleteEvents(rig, one, 4);
        }

        Assert.Single(first.Issued.OfType<UndoOrder>());

        var second = new CheckoutServices();
        using var two = await rig.StartAsync(second);

        var after = await SqlCheckoutRig.HostedState(two);
        Assert.Equal(4, after.SequenceNr);
        var running = Assert.IsType<WfRunning>(after.State.Value);
        Assert.Equal(before.State.Value, running);
        Assert.Equal(repeatAt, running.Delayed);
        Assert.Empty(WorkAttempts.Of(running.Attempts));
        Assert.IsType<Undoing>(((CheckoutState)running.State).Value);

        // The reminder is stored again for the pending date, and the work is not issued before it.
        (await SqlCheckoutRig.AwaitReminder(two, r => r.StoredFor() == repeatAt)).IsWakeFor(repeatAt);
        SqlCheckoutRig.Wake(two, repeatAt - TimeSpan.FromSeconds(1));
        Assert.Equal(4, (await SqlCheckoutRig.HostedState(two)).SequenceNr);
        Assert.Empty(second.Issued);

        SqlCheckoutRig.Wake(two, repeatAt);
        var outcome = await SqlCheckoutRig.Ref(two).WaitForOutcome(SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
        Assert.Equal(new OrderFailed("card declined"), outcome.Value);
        Assert.Equal([new UndoOrder("w1")], second.Issued);
        await two.StopAsync(Ct);
    }
}
