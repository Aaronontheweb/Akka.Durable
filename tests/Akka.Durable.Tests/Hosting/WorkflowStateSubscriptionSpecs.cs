using Akka.Durable.Tests.Fixtures.Checkouts;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// What a caller sees from <c>WatchStateAsync</c>, <c>WaitForStateAsync</c> and <c>WaitForOutcome</c> on a workflow
/// ref, in local and in sharded hosting.
/// </summary>
public abstract class WorkflowStateSubscriptionSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(400);

    // The first message of a sharded host waits for the cluster and the shard coordinator.
    protected override TimeSpan DefaultAskTimeout => TimeSpan.FromSeconds(30);

    private static TaskCompletionSource<CheckoutFact> Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The task has not finished after a short real window.</summary>
    private Task StillWaiting(Task task) =>
        Assert.ThrowsAsync<TimeoutException>(() => task.WaitAsync(Short, TestCancellation));

    private static string Label(StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>> snapshot) =>
        snapshot.State.Value switch
        {
            WorkflowNotStarted => "not started",
            WorkflowRunning<CheckoutState> running => running.StateName,
            WorkflowFinished<CheckoutOutcome> finished => $"finished {finished.Outcome.Value!.GetType().Name}",
            _ => "unknown",
        };

    [Fact]
    public async Task WatchStateAsync_gives_not_started_then_each_state_then_the_outcome_and_ends()
    {
        var reserve = Gate();
        var charge = Gate();
        Services.Reserve = (_, ct) => reserve.Task.WaitAsync(ct);
        Services.Charge = (_, ct) => charge.Task.WaitAsync(ct);
        var checkout = CheckoutRef("w1");
        await using var reader = StreamReader<StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>>
            .Open(checkout.WatchStateAsync, TestCancellation);

        var read = new List<StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>>();
        var first = await reader.NextAsync();
        read.Add(first);
        Assert.IsType<WorkflowNotStarted>(first.State.Value);
        Assert.Equal(0, first.SequenceNr);

        Assert.IsType<Started>((await checkout.Start(new PlaceOrder("w1", 500), AskCancellation())).Value);
        read.AddRange(await reader.ReadUntilAsync(s => Label(s) == nameof(Reserving)));

        reserve.SetResult((CheckoutFact)new Reserved("res-w1"));
        read.AddRange(await reader.ReadUntilAsync(s => Label(s) == nameof(Charging)));

        charge.SetResult((CheckoutFact)new Charged("charge-w1"));
        read.AddRange(await reader.ReadUntilAsync(s => s.State.Value is WorkflowFinished<CheckoutOutcome>));

        // The stream ends on its own, after the finished snapshot.
        await reader.ExpectEndAsync();

        var labels = read.Select(Label).Aggregate(new List<string>(), (all, label) =>
        {
            if (all.Count == 0 || all[^1] != label) all.Add(label);
            return all;
        });
        Assert.Equal(["not started", nameof(Reserving), nameof(Charging), $"finished {nameof(OrderPlaced)}"], labels);

        var finished = Assert.IsType<WorkflowFinished<CheckoutOutcome>>(read[^1].State.Value);
        Assert.Equal(new OrderPlaced("charge-w1"), finished.Outcome.Value);
        Assert.Equal(read.Select(s => s.SequenceNr).Order(), read.Select(s => s.SequenceNr));
        Assert.Equal(await checkout.GetStateAsync(AskCancellation()), read[^1]);
    }

    [Fact]
    public async Task WatchStateAsync_of_a_finished_workflow_gives_the_outcome_and_ends()
    {
        var checkout = CheckoutRef("w2");
        await checkout.Start(new PlaceOrder("w2", 100), AskCancellation());
        await checkout.WaitForOutcome(AskCancellation());

        await using var reader = StreamReader<StateSnapshot<WorkflowStatus<CheckoutState, CheckoutOutcome>>>
            .Open(checkout.WatchStateAsync, TestCancellation);

        var only = await reader.NextAsync();
        Assert.Equal("finished OrderPlaced", Label(only));
        await reader.ExpectEndAsync();
    }

    [Fact]
    public async Task WaitForOutcome_returns_the_outcome_of_a_workflow_that_finishes_later()
    {
        var reserve = Gate();
        Services.Reserve = (_, ct) => reserve.Task.WaitAsync(ct);
        var checkout = CheckoutRef("w3");
        await checkout.Start(new PlaceOrder("w3", 100), AskCancellation());

        var wait = checkout.WaitForOutcome(AskCancellation());
        await StillWaiting(wait);

        reserve.SetResult((CheckoutFact)new Reserved("res-w3"));

        Assert.Equal(new OrderPlaced("charge-w3"), (await wait).Value);
    }

    [Fact]
    public async Task WaitForOutcome_returns_the_outcome_of_a_workflow_that_is_already_finished()
    {
        var checkout = CheckoutRef("w4");
        await checkout.Start(new PlaceOrder("w4", 100), AskCancellation());
        await checkout.WaitForOutcome(AskCancellation());

        Assert.Equal(new OrderPlaced("charge-w4"), (await checkout.WaitForOutcome(AskCancellation())).Value);
    }

    [Fact]
    public async Task WaitForOutcome_throws_InvalidOperationException_for_a_workflow_that_has_not_been_started()
    {
        var checkout = CheckoutRef("never");

        await Assert.ThrowsAsync<InvalidOperationException>(() => checkout.WaitForOutcome(AskCancellation()));
    }

    [Fact]
    public async Task WaitForOutcome_throws_OperationCanceledException_when_its_token_fires()
    {
        var reserve = Gate();
        Services.Reserve = (_, ct) => reserve.Task.WaitAsync(ct);
        var checkout = CheckoutRef("w5");
        await checkout.Start(new PlaceOrder("w5", 100), AskCancellation());
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestCancellation);
        var wait = checkout.WaitForOutcome(cts.Token);
        await StillWaiting(wait);

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
    }

    [Fact]
    public async Task WaitForStateAsync_returns_the_first_status_that_satisfies_the_predicate()
    {
        var charge = Gate();
        Services.Charge = (_, ct) => charge.Task.WaitAsync(ct);
        var checkout = CheckoutRef("w6");

        var wait = checkout.WaitForStateAsync(
            status => status.Value is WorkflowRunning<CheckoutState> { State.Value: Charging }, AskCancellation());
        await checkout.Start(new PlaceOrder("w6", 100), AskCancellation());

        var found = await wait;

        Assert.Equal(nameof(Charging), Label(found));
        Assert.Equal(await checkout.GetStateAsync(AskCancellation()), found);
        charge.SetResult((CheckoutFact)new Charged("c"));
    }

    [Fact]
    public async Task WaitForStateAsync_returns_at_once_when_the_workflow_already_satisfies_the_predicate()
    {
        var checkout = CheckoutRef("w7");

        var found = await checkout.WaitForStateAsync(s => s.Value is WorkflowNotStarted, AskCancellation(TimeSpan.FromSeconds(10)));

        Assert.Equal("not started", Label(found));
        Assert.Equal(0, found.SequenceNr);
    }
}

public sealed class LocalWorkflowStateSubscriptionSpecs(ITestOutputHelper output) : WorkflowStateSubscriptionSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowStateSubscriptionSpecs(ITestOutputHelper output) : WorkflowStateSubscriptionSpecs(output)
{
    protected override bool Sharded => true;
}
