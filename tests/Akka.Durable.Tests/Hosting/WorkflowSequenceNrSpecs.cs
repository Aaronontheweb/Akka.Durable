using System.Runtime.CompilerServices;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Remembers the previous read of a workflow and holds each new one to the contract of
/// <c>SequenceNr</c>: it never goes down, a visible change of status always comes with a larger number, and
/// the same number always comes with the same status.
/// </summary>
internal sealed class SequenceTrail
{
    private object? _status;
    private long _seq = -1;

    public void Check<TState, TOutcome>(StateSnapshot<WorkflowStatus<TState, TOutcome>> read)
        where TState : notnull
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
    }

    /// <summary>What a reader can see of a status, comparable by value: the case, its data and its time.</summary>
    private static object? Visible(object? status) => status switch
    {
        WorkflowRunning<CheckoutState> r => (Unwrap(r.State), r.Since),
        WorkflowRunning<CampaignState> r => (Unwrap(r.State), r.Since),
        WorkflowFinished<CheckoutOutcome> f => (Unwrap(f.Outcome), f.At),
        WorkflowFinished<CampaignOutcome> f => (Unwrap(f.Outcome), f.At),
        _ => status,
    };

    private static object? Unwrap(object? value)
    {
        while (value is IUnion { Value: { } inner }) value = inner;
        return value;
    }
}

/// <summary>
/// The <c>SequenceNr</c> of a workflow read, for the checkout workflow. Every expected number is worked out from
/// the journal the engine writes: the start is one event, each fact the table decides is one event (a move, a
/// stay, a repeat, a finish and an unexpected fact alike), and a fact held while work is in flight is one event
/// when it arrives and one more when it is decided.
/// </summary>
public abstract class WorkflowSequenceNrSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private async Task<StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>> Reach(
        string id, SequenceTrail trail, long sequenceNr)
    {
        StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>? read = null;
        await AwaitAssertAsync(async () =>
        {
            read = await CheckoutRef(id).GetStateAsync(AskCancellation());
            Assert.Equal(sequenceNr, read.SequenceNr);
        }, cancellationToken: TestCancellation);
        trail.Check(read!);
        return read!;
    }

    private static object? Inside(StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>> read) =>
        read.State.Value switch
        {
            WorkflowRunning<CheckoutState> running => ((CheckoutState)running.State).Value,
            WorkflowFinished<CheckoutOutcome> finished => ((CheckoutOutcome)finished.Outcome).Value,
            var other => other,
        };

    [Theory]
    [InlineData("only a read")]
    [InlineData("a fact that is refused")]
    [InlineData("a cancel that is refused")]
    public async Task A_workflow_that_was_never_started_is_at_zero_and_nothing_that_is_refused_moves_it(string refusal)
    {
        var checkout = CheckoutRef("n0");
        var trail = new SequenceTrail();
        Assert.IsType<WorkflowNotStarted>((await Reach("n0", trail, 0)).State.Value);

        switch (refusal)
        {
            case "a fact that is refused":
                Assert.IsType<WorkflowNotStarted>((await checkout.Notify(new Reserved("r"), AskCancellation())).Value);
                break;
            case "a cancel that is refused":
                Assert.IsType<WorkflowNotStarted>((await checkout.Cancel("why", "user-1", AskCancellation())).Value);
                break;
        }

        Assert.IsType<WorkflowNotStarted>((await Reach("n0", trail, 0)).State.Value);
    }

    [Fact]
    public async Task The_number_grows_with_the_start_each_held_fact_each_decided_fact_and_the_finish()
    {
        var reserved = new TaskCompletionSource<CheckoutFact>(TaskCreationOptions.RunContinuationsAsynchronously);
        var refused = new TaskCompletionSource<CheckoutFact>(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource<CheckoutFact>(TaskCreationOptions.RunContinuationsAsynchronously);
        var undos = 0;
        Services.Reserve = (_, ct) => reserved.Task.WaitAsync(ct);
        Services.Undo = (_, ct) => Interlocked.Increment(ref undos) == 1
            ? refused.Task.WaitAsync(ct)
            : gate.Task.WaitAsync(ct);
        var checkout = CheckoutRef("n1");
        var trail = new SequenceTrail();

        // Not started: 0. Start: 1 (WfStarted).
        await Reach("n1", trail, 0);
        Assert.IsType<Started>((await checkout.Start(new PlaceOrder("n1", 100), AskCancellation())).Value);
        var started = await Reach("n1", trail, 1);
        Assert.Equal(new Reserving(), Inside(started));

        // Reads with nothing in between, and a start that is refused, journal nothing.
        await Reach("n1", trail, 1);
        Assert.IsType<AlreadyRunning>((await checkout.Start(new PlaceOrder("n1", 100), AskCancellation())).Value);
        await Reach("n1", trail, 1);

        // A cancel while the reservation is in flight is held: 2 (WfHeld). The status shows nothing of it.
        Assert.IsType<Held>((await checkout.Cancel("changed mind", "user-1", AskCancellation())).Value);
        var held = await Reach("n1", trail, 2);
        Assert.Equal(Inside(started), Inside(held));

        // The reservation is answered: Reserving to Charging (3), then the held cancel is decided in Charging and
        // moves to Undoing (4). One write, two events.
        reserved.SetResult(new Reserved("res-n1"));
        var undoing = await Reach("n1", trail, 4);
        Assert.Equal(new Undoing(new OrderCancelled("changed mind")), Inside(undoing));

        // The first undo is refused and the table repeats the state: 5 (WfRepeated). A reader sees the same
        // status at a higher number. The repeated undo is held open.
        refused.SetResult(new UndoRefused());
        var repeated = await Reach("n1", trail, 5);
        Assert.Equal(Inside(undoing), Inside(repeated));

        // The undo is done: Finish, 6.
        gate.SetResult(new Undone());
        var finished = await Reach("n1", trail, 6);
        Assert.Equal(new OrderCancelled("changed mind"), Inside(finished));

        // A finished workflow is closed: refusals journal nothing.
        Assert.IsType<AlreadyFinished<CheckoutOutcome>>((await checkout.Start(new PlaceOrder("n1", 100), AskCancellation())).Value);
        Assert.IsType<AlreadyFinished<CheckoutOutcome>>((await checkout.Notify(new Undone(), AskCancellation())).Value);
        Assert.IsType<AlreadyFinished<CheckoutOutcome>>((await checkout.Cancel("again", "user-1", AskCancellation())).Value);
        await Reach("n1", trail, 6);
        Assert.Equal(2, undos);
    }

    [Fact]
    public async Task A_fact_that_keeps_the_state_and_an_unexpected_fact_each_add_one_and_a_move_back_adds_one_more()
    {
        Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        var checkout = CheckoutRef("n2");
        var trail = new SequenceTrail();
        await checkout.Start(new PlaceOrder("n2", 100), AskCancellation());
        await SettledIn<NeedsAttention>("n2");

        // Start 1, Reserved 2 (move), the charge gave up 3 (move to NeedsAttention), the ticket opened 4 (Stay).
        var stuck = await Reach("n2", trail, 4);
        Assert.IsType<NeedsAttention>(Inside(stuck));

        // A cancel while waiting for a person: decided at once, and kept (Stay): 5.
        Assert.IsType<Delivered>((await checkout.Cancel("later", "user-2", AskCancellation())).Value);
        Assert.Equal(Inside(stuck), Inside(await Reach("n2", trail, 5)));

        // A fact the table marks as unexpected is journaled and changes nothing: 6.
        Assert.IsType<Delivered>((await checkout.Notify(new Reserved("late"), AskCancellation())).Value);
        Assert.Equal(Inside(stuck), Inside(await Reach("n2", trail, 6)));

        // A person says retry: the state before is entered again, with a new time: 7.
        Services.Charge = (_, ct) => Never(ct);
        Assert.IsType<Delivered>((await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);
        var retried = await Reach("n2", trail, 7);
        Assert.Equal(new Charging("res-n2"), Inside(retried));
    }

    public static TheoryData<string, long> RestartRows => new()
    {
        { "waiting for a person", 4 },
        { "finished", 3 },
    };

    [Theory]
    [MemberData(nameof(RestartRows))]
    public async Task The_number_survives_a_restart_of_the_hosting_entity(string shape, long expected)
    {
        var id = "n3-" + shape.Replace(' ', '-');
        if (shape == "finished")
        {
            // Start 1, Reserved 2, Charged 3 (Finish).
            await CheckoutRef(id).Start(new PlaceOrder(id, 100), AskCancellation());
        }
        else
        {
            Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
            await CheckoutRef(id).Start(new PlaceOrder(id, 100), AskCancellation());
            await SettledIn<NeedsAttention>(id);
        }

        var trail = new SequenceTrail();
        var before = await Reach(id, trail, expected);

        await RestartWorkflow(id);

        var after = await Reach(id, trail, expected);
        Assert.Equal(Inside(before), Inside(after));
    }

    [Fact]
    public async Task A_restart_moves_the_number_of_a_workflow_with_work_in_flight_because_the_work_is_given_up()
    {
        Services.Reserve = (_, ct) => Never(ct);
        var checkout = CheckoutRef("n4");
        var trail = new SequenceTrail();
        await checkout.Start(new PlaceOrder("n4", 100), AskCancellation());
        await Reach("n4", trail, 1);

        await StopWorkflow("n4");

        // The recovered workflow cannot know the reservation's fate: that work is answered as given up, which
        // the table moves to NeedsAttention (2), and the ticket then opens (3, Stay).
        var recovered = await Reach("n4", trail, 3);
        Assert.IsType<NeedsAttention>(Inside(recovered));
    }
}

public sealed class LocalWorkflowSequenceNrSpecs(ITestOutputHelper output) : WorkflowSequenceNrSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowSequenceNrSpecs(ITestOutputHelper output) : WorkflowSequenceNrSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>The <c>SequenceNr</c> of a workflow read, for the campaign workflow: timers and a wake that journals nothing.</summary>
public abstract class WorkflowSequenceNrTimerSpecs(ITestOutputHelper output) : CampaignWorkflowKit(output)
{
    private static readonly TimeSpan Long = TimeSpan.FromHours(1);

    private async Task<StateSnapshot<WorkflowStatus<CampaignState, CampaignOutcome>>> Reach(
        string id, SequenceTrail trail, long sequenceNr, TimeSpan? within = null)
    {
        StateSnapshot<WorkflowStatus<CampaignState, CampaignOutcome>>? read = null;
        await AwaitAssertAsync(async () =>
        {
            read = await CampaignRef(id).GetStateAsync(AskCancellation());
            Assert.Equal(sequenceNr, read.SequenceNr);
        }, within, cancellationToken: TestCancellation);
        trail.Check(read!);
        return read!;
    }

    [Fact]
    public async Task A_wake_that_journals_nothing_does_not_move_the_number_and_a_fact_that_finishes_adds_one()
    {
        var campaign = CampaignRef("m1");
        var trail = new SequenceTrail();
        await campaign.Start(new Invoice("m1", DateTimeOffset.UtcNow + Long, Long, Long, Reminders: 1), AskCancellation());
        await Reach("m1", trail, 1);

        // Nothing is due: the wake is acknowledged and journals nothing.
        Wake("m1");
        await Reach("m1", trail, 1);

        // The payment is decided at once and finishes the workflow: 2.
        Assert.IsType<Delivered>((await campaign.Notify(new Paid(), AskCancellation())).Value);
        var finished = await Reach("m1", trail, 2);
        Assert.IsType<WorkflowFinished<CampaignOutcome>>(finished.State.Value);
    }

    [Fact]
    public async Task A_timer_that_fires_adds_its_fact_and_the_facts_of_the_work_that_follows()
    {
        var dueAt = DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(300);
        await CampaignRef("m2").Start(new Invoice("m2", dueAt, Long, Long, Reminders: 2), AskCancellation());

        // Start 1, the due date 2 (Waiting to Reminding), the reminder sent 3 (Reminding to Pausing).
        var read = await Reach("m2", new SequenceTrail(), 3, TimeSpan.FromSeconds(10));
        var running = Assert.IsType<WorkflowRunning<CampaignState>>(read.State.Value);
        Assert.Equal(new Pausing(1), ((CampaignState)running.State).Value);
        Assert.True(running.Since >= dueAt);
    }
}

public sealed class LocalWorkflowSequenceNrTimerSpecs(ITestOutputHelper output) : WorkflowSequenceNrTimerSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowSequenceNrTimerSpecs(ITestOutputHelper output) : WorkflowSequenceNrTimerSpecs(output)
{
    protected override bool Sharded => true;
}
