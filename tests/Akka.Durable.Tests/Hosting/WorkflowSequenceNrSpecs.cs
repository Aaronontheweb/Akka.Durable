using System.Runtime.CompilerServices;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

using Read = StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>;

/// <summary>Holds each read to what a reader relies on: the number never goes down, a visible change comes with a larger one, the same one never shows two statuses.</summary>
internal sealed class SequenceTrail
{
    private object? _status;
    private long _seq = -1;

    public Read Check(Read read)
    {
        var status = Visible(read.State.Value);
        if (_seq >= 0)
        {
            Assert.True(read.SequenceNr >= _seq, $"The number went down, from {_seq} to {read.SequenceNr}.");
            if (!Equals(status, _status))
                Assert.True(read.SequenceNr > _seq, $"The status changed, but the number stayed at {_seq}.");
            if (read.SequenceNr == _seq)
                Assert.Equal(_status, status);
        }

        (_status, _seq) = (status, read.SequenceNr);
        return read;
    }

    public static object? Visible(object? status) => status switch
    {
        WorkflowRunning<CheckoutState> r => (Unwrap(r.State), r.Since),
        WorkflowFinished<CheckoutOutcome> f => (Unwrap(f.Outcome), f.At),
        _ => status,
    };

    private static object? Unwrap(object? value) => value is IUnion { Value: { } inner } ? Unwrap(inner) : value;
}

public sealed class WorkflowSequenceNrSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private static TaskCompletionSource<CheckoutFact> Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task<Read> Latest(string id) => CheckoutRef(id).GetStateAsync(AskCancellation());

    private async Task<Read> Until(string id, SequenceTrail trail, Func<Read, bool> ready)
    {
        Read? found = null;
        await AwaitAssertAsync(async () => Assert.True(ready(found = await Latest(id))), cancellationToken: TestCancellation);
        return trail.Check(found!);
    }

    [Theory]
    [InlineData("only a read")]
    [InlineData("a fact that is refused")]
    [InlineData("a cancel that is refused")]
    public async Task A_workflow_that_was_never_started_reads_zero_and_a_refusal_does_not_move_it(string refusal)
    {
        var checkout = CheckoutRef("n0");
        Assert.Equal(0, (await Latest("n0")).SequenceNr);
        if (refusal == "a fact that is refused") await checkout.Notify(new Reserved("r"), AskCancellation());
        if (refusal == "a cancel that is refused") await checkout.Cancel("why", "user-1", AskCancellation());

        Assert.Equal(0, (await Latest("n0")).SequenceNr);
    }

    [Fact]
    public async Task Along_a_run_from_start_to_finish_a_reader_can_tell_from_the_number_whether_anything_changed()
    {
        TaskCompletionSource<CheckoutFact> reserved = Gate(), refused = Gate(), gate = Gate();
        var undos = 0;
        Services.Reserve = (_, ct) => reserved.Task.WaitAsync(ct);
        Services.Undo = (_, ct) => (Interlocked.Increment(ref undos) == 1 ? refused : gate).Task.WaitAsync(ct);
        var (checkout, trail) = (CheckoutRef("n1"), new SequenceTrail());
        var notStarted = trail.Check(await Latest("n1"));
        await checkout.Start(new PlaceOrder("n1", 100), AskCancellation());
        var started = await Until("n1", trail, r => r.SequenceNr > notStarted.SequenceNr);

        Assert.Equal(started.SequenceNr, trail.Check(await Latest("n1")).SequenceNr); // nothing between two reads

        Assert.IsType<Held>((await checkout.Cancel("changed mind", "user-1", AskCancellation())).Value);
        var held = await Until("n1", trail, r => r.SequenceNr > started.SequenceNr); // moves, shows nothing
        Assert.Equal(SequenceTrail.Visible(started.State.Value), SequenceTrail.Visible(held.State.Value));

        reserved.SetResult(new Reserved("res-n1"));
        var undoing = await Until("n1", trail, r => r.State.Value is WorkflowRunning<CheckoutState> { State.Value: Undoing });

        refused.SetResult(new UndoRefused());
        var repeated = await Until("n1", trail, r => r.SequenceNr > undoing.SequenceNr); // Again: same status
        Assert.Equal(SequenceTrail.Visible(undoing.State.Value), SequenceTrail.Visible(repeated.State.Value));

        gate.SetResult(new Undone());
        var finished = await Until("n1", trail, r => r.State.Value is WorkflowFinished<CheckoutOutcome>);
        Assert.True(finished.SequenceNr > repeated.SequenceNr);
    }

    [Theory]
    [InlineData("waiting for a person")]
    [InlineData("finished")]
    public async Task The_number_is_the_same_after_a_restart_of_the_hosting_entity(string shape)
    {
        var id = "n3-" + shape.Replace(' ', '-');
        if (shape != "finished") Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        await CheckoutRef(id).Start(new PlaceOrder(id, 100), AskCancellation());
        if (shape != "finished") await SettledIn<NeedsAttention>(id);
        var trail = new SequenceTrail();
        var before = await Until(id, trail, r => r.State.Value is not WorkflowRunning<CheckoutState> { State.Value: Reserving or Charging });

        await RestartWorkflow(id);

        var after = trail.Check(await Latest(id));
        Assert.Equal(before.SequenceNr, after.SequenceNr);
        Assert.Equal(SequenceTrail.Visible(before.State.Value), SequenceTrail.Visible(after.State.Value));
    }
}
