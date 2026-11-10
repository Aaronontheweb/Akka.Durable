using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Facts that come from outside the workflow: <c>Notify</c>, <c>Cancel</c>, and a message type the
/// definition accepts. A fact that arrives while work is in flight is held, and decided when the work settles.
/// </summary>
public abstract class WorkflowFactsFromOutsideSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    protected override WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        Checkout.WithOperatorTool;

    private IWorkflowRef<PlaceOrder, CheckoutFact, CheckoutOutcome> CheckoutWithFacts(string id) =>
        WorkflowRef<PlaceOrder, CheckoutFact, CheckoutOutcome>(id);

    /// <summary>Runs a checkout until its charge fails and it waits for a person, with nothing in flight.</summary>
    private async Task<WfRunning> WaitingForAPerson(string id)
    {
        Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        await CheckoutWithFacts(id).Start(new PlaceOrder(id, 100), AskCancellation());
        return await SettledIn<NeedsAttention>(id);
    }

    private static Func<ChargeCard, CancellationToken, Task<CheckoutFact>> ChargeSucceeds =>
        (effect, _) => Task.FromResult<CheckoutFact>(new Charged($"charge-{effect.Order}"));

    private static TaskCompletionSource<CheckoutFact> Pending() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task A_fact_sent_while_nothing_is_in_flight_is_decided_at_once()
    {
        await WaitingForAPerson("f1");
        Services.Charge = ChargeSucceeds;
        var checkout = CheckoutWithFacts("f1");

        Assert.IsType<Delivered>((await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);

        // Back to the charge, which is issued again and goes through this time.
        Assert.Equal(new OrderPlaced("charge-f1"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal(
            [new ReserveStock("f1"), new ChargeCard("f1", 100), new OpenSupportTicket("f1", "ChargeCard Failed"), new ChargeCard("f1", 100)],
            Services.Issued);
    }

    [Fact]
    public async Task A_cancel_while_work_is_in_flight_is_held_and_decided_before_the_next_work_is_issued()
    {
        var reserved = Pending();
        Services.Reserve = (_, ct) => reserved.Task.WaitAsync(ct);
        var checkout = CheckoutWithFacts("f2");
        await checkout.Start(new PlaceOrder("f2", 100), AskCancellation());

        Assert.IsType<Held>((await checkout.Cancel("customer asked", AskCancellation())).Value);
        reserved.SetResult(new Reserved("res-f2"));

        Assert.Equal(new OrderCancelled("customer asked"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        var finished = Assert.IsType<WorkflowFinished<CheckoutOutcome>>((await checkout.Status(AskCancellation())).Value);
        Assert.Equal(OutcomeKind.Cancelled, finished.Kind);

        // The reservation moved the checkout to Charging, but the cancel was decided before the charge was issued.
        Assert.Equal([new ReserveStock("f2"), new UndoOrder("f2")], Services.Issued);
    }

    [Fact]
    public async Task Held_facts_are_decided_in_the_order_they_arrived()
    {
        var reserved = Pending();
        Services.Reserve = (_, ct) => reserved.Task.WaitAsync(ct);
        var checkout = CheckoutWithFacts("f3");
        await checkout.Start(new PlaceOrder("f3", 100), AskCancellation());

        Assert.IsType<Held>((await checkout.Cancel("first", AskCancellation())).Value);
        Assert.IsType<Held>((await checkout.Cancel("second", AskCancellation())).Value);
        reserved.SetResult(new Reserved("res-f3"));

        Assert.Equal(new OrderCancelled("first"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Single(Services.Issued.OfType<UndoOrder>());
    }

    [Fact]
    public async Task A_cancel_the_table_answers_with_stay_changes_nothing_but_the_journal()
    {
        await WaitingForAPerson("f4");
        var before = await HostedState("f4");
        var issuedBefore = Services.Issued;

        Assert.IsType<Delivered>((await CheckoutWithFacts("f4").Cancel("customer asked", AskCancellation())).Value);

        var after = await HostedState("f4");
        Assert.Equal(before.SequenceNr + 1, after.SequenceNr);
        Assert.Equal(before.State, after.State);
        Assert.Equal(issuedBefore, Services.Issued);
    }

    [Fact]
    public async Task A_workflow_that_was_never_started_refuses_facts()
    {
        var checkout = CheckoutWithFacts("f5");

        Assert.IsType<WorkflowNotStarted>((await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);
        Assert.IsType<WorkflowNotStarted>((await checkout.Cancel("customer asked", AskCancellation())).Value);

        var state = await HostedState("f5");
        Assert.Equal(0, state.SequenceNr);
        Assert.IsType<WfNotStarted>(state.State.Value);
    }

    [Fact]
    public async Task A_finished_workflow_is_closed_and_says_how_it_ended()
    {
        var checkout = CheckoutWithFacts("f6");
        await checkout.Start(new PlaceOrder("f6", 100), AskCancellation());
        await checkout.WaitForOutcome(Poll, AskCancellation());
        var before = await HostedState("f6");

        var notified = Assert.IsType<AlreadyFinished<CheckoutOutcome>>(
            (await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);
        var cancelled = Assert.IsType<AlreadyFinished<CheckoutOutcome>>(
            (await checkout.Cancel("customer asked", AskCancellation())).Value);

        Assert.Equal(new OrderPlaced("charge-f6"), notified.Outcome.Value);
        Assert.Equal(new OrderPlaced("charge-f6"), cancelled.Outcome.Value);
        Assert.Equal(before.SequenceNr, (await HostedState("f6")).SequenceNr);
    }

    [Fact]
    public async Task A_held_fact_survives_a_restart_and_is_decided_after_the_interrupted_work_gives_up()
    {
        Services.Charge = (_, ct) => Never(ct);
        var checkout = CheckoutWithFacts("f7");
        await checkout.Start(new PlaceOrder("f7", 100), AskCancellation());
        await AwaitRunning("f7", running => Assert.IsType<Charging>(((CheckoutState)running.State).Value));

        Assert.IsType<Held>((await checkout.Cancel("customer asked", AskCancellation())).Value);
        await AwaitRunning("f7", running => Assert.Single(running.Held));

        await StopWorkflow("f7");

        // The interrupted charge gives up and the table sends the checkout to a person. The held cancel is
        // decided there, where the table keeps the state, and only then is the ticket issued.
        var recovered = await SettledIn<NeedsAttention>("f7");
        Assert.Equal(
            new NeedsAttention(new Charging("res-f7"), "ChargeCard Interrupted"),
            ((CheckoutState)recovered.State).Value);
        Assert.Empty(recovered.Held);
        Assert.Single(Services.Issued.OfType<OpenSupportTicket>());

        // The start, the reservation, the held cancel, the give-up, the cancel decided, and the ticket.
        Assert.Equal(6, (await HostedState("f7")).SequenceNr);
    }

    [Fact]
    public async Task A_message_the_workflow_accepts_is_decided_as_the_fact_it_maps_to()
    {
        await WaitingForAPerson("f8");
        var region = Host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<PlaceOrder, CheckoutOutcome>>>().ActorRef;

        // An operator tool sends its own message type to the workflow's address.
        region.Tell(new ShardingEnvelope("f8", new OperatorDecision(Retry: false)), TestActor);

        Assert.Equal(new OrderFailed("ChargeCard Failed"), (await CheckoutWithFacts("f8").WaitForOutcome(Poll, AskCancellation())).Value);
        Assert.Equal(new UndoOrder("f8"), Services.Issued[^1]);

        // The sender did not ask, so it gets no reply.
        await ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100), TestCancellation);
    }
}

public sealed class LocalWorkflowFactsFromOutsideSpecs(ITestOutputHelper output) : WorkflowFactsFromOutsideSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowFactsFromOutsideSpecs(ITestOutputHelper output) : WorkflowFactsFromOutsideSpecs(output)
{
    protected override bool Sharded => true;
}
