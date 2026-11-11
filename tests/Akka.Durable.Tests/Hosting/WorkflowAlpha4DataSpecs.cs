using System.Buffers;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures.Checkouts;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Data that 0.1.0-alpha.4 wrote: a <see cref="WfRunning"/> without the attempts and the delay, a
/// <see cref="WfRepeated"/> without its date. The bytes are written here from the field numbers of that version
/// (<c>WfRunning</c> 0 to 6, <c>WfRepeated</c> none) with a MessagePack writer, put in place of what a real run wrote
/// to SQLite, and read by a new actor system that goes on with the workflow.
/// </summary>
public sealed class WorkflowAlpha4DataSpecs
{
    private const int LibrarySerializer = 22560;
    private const int CheckoutSerializer = 22601;

    private static CancellationToken Ct => SqlCheckoutRig.Ct;

    private static readonly TimeSpan Fuzz = TimeSpan.FromMilliseconds(250);

    private delegate void Write(ref MessagePackWriter writer);

    private static byte[] Bytes(Write write)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        write(ref writer);
        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>A union value: its manifest, then its fields.</summary>
    private static byte[] Union(string manifest, byte[] fields) => Bytes((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(2);
        w.Write(1);
        w.Write(manifest);
        w.Write(2);
        w.WriteRaw(fields);
    });

    /// <summary>A value of type object: the serializer that wrote it, its manifest, and its bytes.</summary>
    private static byte[] Envelope(int serializer, string manifest, byte[] payload) => Bytes((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(3);
        w.Write(1);
        w.Write(serializer);
        w.Write(2);
        w.Write(manifest);
        w.Write(3);
        w.Write(payload);
    });

    private static byte[] Date(DateTimeOffset value) => Bytes((ref MessagePackWriter w) =>
    {
        w.WriteArrayHeader(2);
        w.WriteUInt64((ulong)value.UtcTicks);
        w.Write(0);
    });

    private static byte[] Raw(Write write) => Bytes(write);

    private static byte[] Order() => Envelope(CheckoutSerializer, "checkout-place-order-v1", Bytes((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(2);
        w.Write(0);
        w.Write("w1");
        w.Write(1);
        w.Write(100);
    }));

    /// <summary>
    /// <c>WfRunning</c> as 0.1.0-alpha.4 wrote it: a workflow in Charging, its charge in flight, with the fields 0 to 6.
    /// </summary>
    private static byte[] OldRunningCharging(DateTimeOffset enteredAt) => Bytes((ref MessagePackWriter w) =>
    {
        var charging = Envelope(CheckoutSerializer, "checkout-state-v1", Union("checkout-charging-v1", Bytes((ref MessagePackWriter f) =>
        {
            f.WriteMapHeader(1);
            f.Write(0);
            f.Write("res-w1");
        })));
        w.WriteMapHeader(7);
        w.Write(0);
        w.WriteRaw(Order());
        w.Write(1);
        w.WriteRaw(charging);
        w.Write(2);
        w.Write(2);
        w.Write(3);
        w.WriteRaw(Date(enteredAt));
        w.Write(4);
        w.WriteArrayHeader(0);
        w.Write(5);
        w.WriteArrayHeader(0);
        w.Write(6);
        w.WriteRaw(Date(enteredAt));
    });

    /// <summary>The snapshot of an entity: the state in a <c>WfState</c>, and no pending effects.</summary>
    private static byte[] SnapshotOf(byte[] running) => Bytes((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(2);
        w.Write(0);
        w.WriteRaw(Envelope(LibrarySerializer, "durable-wf-state-v1", Union("durable-wf-running-v1", running)));
        w.Write(1);
        w.WriteArrayHeader(0);
    });

    /// <summary><c>WfRepeated</c> as 0.1.0-alpha.4 wrote it: a record without fields.</summary>
    private static byte[] OldRepeated() => Bytes((ref MessagePackWriter w) => w.WriteMapHeader(0));

    /// <summary>The event that decided <c>UndoRefused</c> in Undoing and kept the state: a <c>WfFactRecorded</c> with an old repeat, as a <c>WfEvent</c>.</summary>
    private static byte[] OldRefusedUndoRepeated(DateTimeOffset at) => Union("durable-wf-fact-v1", Bytes(
        (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(5);
            w.Write(0);
            w.WriteRaw(Envelope(CheckoutSerializer, "checkout-fact-v1", Union("checkout-undo-refused-v1", Raw((ref MessagePackWriter f) => f.WriteMapHeader(0)))));
            w.Write(1);
            w.Write(0);
            w.Write(2);
            w.WriteRaw(Date(at));
            w.Write(3);
            w.WriteRaw(Union("durable-wf-repeated-v1", OldRepeated()));
            w.Write(4);
            w.Write(false);
        }));

    private static object Read(IHost host, byte[] bytes, string manifest) =>
        host.Services.GetRequiredService<ActorSystem>().Serialization.Deserialize(bytes, LibrarySerializer, manifest);

    [Fact]
    public async Task A_running_state_written_without_attempts_and_delay_is_read_with_both_empty_and_the_workflow_goes_on()
    {
        // The first system: the charge hangs, and a snapshot is taken with the second event (the start, the reservation).
        using var rig = new SqlCheckoutRig { SnapshotEvery = 2, EffectTimeout = TimeSpan.FromSeconds(30) };
        var first = new CheckoutServices { Charge = (_, ct) => Task.Delay(Timeout.Infinite, ct).ContinueWith<CheckoutFact>(_ => default) };
        using (var one = await rig.StartAsync(first))
        {
            await SqlCheckoutRig.Ref(one).Start(new PlaceOrder("w1", 100), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
            await SqlCheckoutRig.Until(() => rig.Rows("snapshot") == 1 && first.Issued.OfType<ChargeCard>().Any(), "the snapshot and the charge");
            await one.StopAsync(Ct);
        }

        // What alpha.4 would have written in its place, and the events below it gone.
        var issuedAt = DateTimeOffset.UtcNow;
        var old = SnapshotOf(OldRunningCharging(issuedAt));
        Assert.Equal(1, rig.Execute("UPDATE snapshot SET snapshot = @bytes WHERE persistence_id = @id AND sequence_number = 2",
            ("@bytes", old), ("@id", SqlCheckoutRig.PersistenceId)));
        rig.Execute("DELETE FROM journal WHERE persistence_id = @id", ("@id", SqlCheckoutRig.PersistenceId));

        // The charge has a retry policy now. The attempt that the old state says is in flight was cut off by the restart.
        rig.ChargePolicy = Retry.Backoff(SqlCheckoutRig.Minute, 5 * SqlCheckoutRig.Minute, 3);
        var second = new CheckoutServices();
        using var two = await rig.StartAsync(second);

        // Read on its own: the fields that the bytes do not have are empty.
        var snapshot = Assert.IsType<EntitySnapshot>(Read(two, old, "durable-entity-snapshot-v1"));
        var read = Assert.IsType<WfRunning>(Assert.IsType<WfState>(snapshot.State).Value);
        Assert.Empty(WorkAttempts.Of(read.Attempts));
        Assert.Null(read.Delayed);
        Assert.Equal(issuedAt, read.IssuedAt);
        Assert.Equal(new Charging("res-w1"), ((CheckoutState)read.State).Value);

        // The workflow counts the cut-off charge as attempt 1, started when the work was issued, and waits.
        var recovered = await SqlCheckoutRig.AwaitState(two, r => WorkAttempts.Find(r.Attempts, 0) is { NextAt: not null });
        var running = (WfRunning)recovered.State.Value;
        var attempt = Assert.IsType<WfAttempt>(Assert.Single(WorkAttempts.Of(running.Attempts)));
        var next = issuedAt + TimeSpan.FromSeconds(30) + SqlCheckoutRig.Minute;
        Assert.Equal(new WfAttempt(0, 1, issuedAt, attempt.NextAt), attempt);
        Assert.InRange(attempt.NextAt!.Value, next - Fuzz, next + Fuzz);
        Assert.Equal(2, running.Entry);
        Assert.Empty(second.Issued);

        // At its time attempt 2 runs and the workflow decides its fact.
        SqlCheckoutRig.Wake(two, attempt.NextAt.Value);
        var outcome = await SqlCheckoutRig.Ref(two).WaitForOutcome(TimeSpan.FromMilliseconds(20), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
        Assert.Equal(new OrderPlaced("charge-w1"), outcome.Value);
        Assert.Equal(2, Assert.Single(SqlCheckoutRig.Deliveries<ChargeCard>(second)).Attempt);
        await two.StopAsync(Ct);
    }

    [Fact]
    public async Task A_repeat_written_without_a_date_is_read_as_a_repeat_at_once_and_the_workflow_goes_on()
    {
        // The first system: the charge is declined, the undo is refused once (the repeat is journaled as the fourth
        // event), and the second undo hangs.
        using var rig = new SqlCheckoutRig { EffectTimeout = TimeSpan.FromSeconds(30) };
        var undone = 0;
        var first = new CheckoutServices
        {
            Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("card declined")),
            Undo = (_, ct) => ++undone == 1
                ? Task.FromResult<CheckoutFact>(new UndoRefused())
                : Task.Delay(Timeout.Infinite, ct).ContinueWith<CheckoutFact>(_ => default),
        };
        using (var one = await rig.StartAsync(first))
        {
            await SqlCheckoutRig.Ref(one).Start(new PlaceOrder("w1", 100), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
            await SqlCheckoutRig.Until(() => first.Issued.OfType<UndoOrder>().Count() == 2, "the second undo");
            Assert.Equal(4, rig.Rows("journal"));
            await one.StopAsync(Ct);
        }

        // The fourth event as alpha.4 wrote it.
        var repeatedAt = DateTimeOffset.UtcNow;
        Assert.Equal(1, rig.Execute("UPDATE journal SET message = @bytes WHERE persistence_id = @id AND sequence_number = 4",
            ("@bytes", OldRefusedUndoRepeated(repeatedAt)), ("@id", SqlCheckoutRig.PersistenceId)));

        // The undo has a retry policy now. The work that the repeat issued was cut off by the restart.
        rig.UndoPolicy = Retry.Backoff(SqlCheckoutRig.Minute, 5 * SqlCheckoutRig.Minute, 3);
        var second = new CheckoutServices();
        using var two = await rig.StartAsync(second);

        // Read on its own: the repeat has no date.
        var step = Assert.IsType<WfRepeated>(Assert.IsType<WfStep>(Read(two, Union("durable-wf-repeated-v1", OldRepeated()), "durable-wf-step-v1")).Value);
        Assert.Null(step.Until);

        // Replayed: the repeat issued the undo at its own time, with nothing delayed and the attempts counted afresh.
        var recovered = await SqlCheckoutRig.AwaitState(two, r => WorkAttempts.Find(r.Attempts, 0) is { NextAt: not null });
        var running = (WfRunning)recovered.State.Value;
        Assert.Equal(4, running.Entry);
        Assert.Null(running.Delayed);
        var attempt = Assert.IsType<WfAttempt>(Assert.Single(WorkAttempts.Of(running.Attempts)));
        var next = repeatedAt + TimeSpan.FromSeconds(30) + SqlCheckoutRig.Minute;
        Assert.Equal(new WfAttempt(0, 1, repeatedAt, attempt.NextAt), attempt);
        Assert.InRange(attempt.NextAt!.Value, next - Fuzz, next + Fuzz);
        Assert.Empty(second.Issued);

        // At its time attempt 2 of the undo runs, and the workflow ends with the outcome it carried.
        SqlCheckoutRig.Wake(two, attempt.NextAt.Value);
        var outcome = await SqlCheckoutRig.Ref(two).WaitForOutcome(TimeSpan.FromMilliseconds(20), SqlCheckoutRig.Within(TimeSpan.FromSeconds(30)));
        Assert.Equal(new OrderFailed("card declined"), outcome.Value);
        Assert.Equal([new UndoOrder("w1")], second.Issued);
        Assert.Equal(2, Assert.Single(SqlCheckoutRig.Deliveries<UndoOrder>(second)).Attempt);
        await two.StopAsync(Ct);
    }
}
