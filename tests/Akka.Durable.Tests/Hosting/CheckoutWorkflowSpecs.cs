using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Durable.Tests.Fixtures.Echoes;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The checkout workflow hosted through the public API, the way an application would host it: started
/// through a workflow ref, with its work run by registered effect handlers.
/// </summary>
public abstract class CheckoutWorkflowSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private static readonly TimeSpan NoChangeWindow = TimeSpan.FromMilliseconds(300);

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddCheckoutHandlers().AddScoped<IEffectHandler<Repeat, EchoFact>, RepeatHandler>();

    // A second workflow type in the same actor system.
    protected override void ConfigureMore(AkkaConfigurationBuilder builder) =>
        builder
            .WithCustomSerializer(EchoSerializer.CreateSerializerDetails)
            .WithDurableWorkflow(Echo.Definition, Sharding());

    [Fact]
    public async Task A_workflow_runs_its_work_in_order_and_ends_with_its_outcome()
    {
        var checkout = CheckoutRef("o1");

        Assert.IsType<Started>((await checkout.Start(new PlaceOrder("o1", 4200), AskCancellation())).Value);
        var outcome = await checkout.WaitForOutcome(Poll, AskCancellation());

        Assert.Equal(new OrderPlaced("charge-o1"), outcome.Value);
        Assert.Equal([new ReserveStock("o1"), new ChargeCard("o1", 4200)], Services.Issued);

        var finished = Assert.IsType<WorkflowFinished<CheckoutOutcome>>((await checkout.Status(AskCancellation())).Value);
        Assert.Equal(new OrderPlaced("charge-o1"), finished.Outcome.Value);
        Assert.Equal(OutcomeKind.Succeeded, finished.Kind);
    }

    [Fact]
    public async Task A_workflow_that_was_never_started_says_so()
    {
        var checkout = CheckoutRef("never");

        Assert.IsType<WorkflowNotStarted>((await checkout.Status(AskCancellation())).Value);
        await Assert.ThrowsAsync<InvalidOperationException>(() => checkout.WaitForOutcome(Poll, AskCancellation()));
    }

    [Fact]
    public async Task Only_the_first_start_begins_a_run()
    {
        var reserved = new TaskCompletionSource<CheckoutFact>(TaskCreationOptions.RunContinuationsAsynchronously);
        Services.Reserve = (_, ct) => reserved.Task.WaitAsync(ct);
        var checkout = CheckoutRef("o2");
        var order = new PlaceOrder("o2", 100);

        Assert.IsType<Started>((await checkout.Start(order, AskCancellation())).Value);
        Assert.IsType<AlreadyRunning>((await checkout.Start(order, AskCancellation())).Value);

        var running = Assert.IsType<WorkflowRunning>((await checkout.Status(AskCancellation())).Value);
        Assert.Equal(nameof(Reserving), running.State);

        reserved.SetResult(new Reserved("res-o2"));
        await checkout.WaitForOutcome(Poll, AskCancellation());

        // A finished workflow is closed: its id is used once.
        var again = Assert.IsType<AlreadyFinished<CheckoutOutcome>>((await checkout.Start(order, AskCancellation())).Value);
        Assert.Equal(new OrderPlaced("charge-o2"), again.Outcome.Value);
        Assert.Single(Services.Issued.OfType<ReserveStock>());
    }

    [Fact]
    public async Task A_declined_charge_is_undone_and_the_order_fails()
    {
        Services.Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("no funds"));
        var checkout = CheckoutRef("o3");

        await checkout.Start(new PlaceOrder("o3", 100), AskCancellation());
        var outcome = await checkout.WaitForOutcome(Poll, AskCancellation());

        Assert.Equal(new OrderFailed("no funds"), outcome.Value);
        Assert.Equal([new ReserveStock("o3"), new ChargeCard("o3", 100), new UndoOrder("o3")], Services.Issued);

        var finished = Assert.IsType<WorkflowFinished<CheckoutOutcome>>((await checkout.Status(AskCancellation())).Value);
        Assert.Equal(OutcomeKind.Failed, finished.Kind);
    }

    [Fact]
    public async Task Again_issues_the_work_of_the_state_once_more()
    {
        var undos = 0;
        Services.Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("no funds"));
        Services.Undo = (_, _) => Task.FromResult<CheckoutFact>(
            Interlocked.Increment(ref undos) == 1 ? new UndoRefused() : new Undone());
        var checkout = CheckoutRef("o4");

        await checkout.Start(new PlaceOrder("o4", 100), AskCancellation());
        await checkout.WaitForOutcome(Poll, AskCancellation());

        Assert.Equal(2, Services.Issued.OfType<UndoOrder>().Count());
    }

    [Fact]
    public async Task Work_whose_handler_throws_is_not_repeated_and_the_table_decides()
    {
        Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        var checkout = CheckoutRef("o5");

        await checkout.Start(new PlaceOrder("o5", 100), AskCancellation());
        var running = await SettledIn<NeedsAttention>("o5");

        // The state the workflow was in is kept, and the work of the new state ran: a ticket for a person.
        Assert.Equal(new NeedsAttention(new Charging("res-o5"), "ChargeCard Failed"), ((CheckoutState)running.State).Value);
        Assert.Equal(new OpenSupportTicket("o5", "ChargeCard Failed"), Services.Issued[^1]);
        Assert.Single(Services.Issued.OfType<ChargeCard>());

        var status = Assert.IsType<WorkflowRunning>((await checkout.Status(AskCancellation())).Value);
        Assert.Equal(nameof(NeedsAttention), status.State);
    }

    [Fact]
    public async Task A_fact_the_table_marks_as_unexpected_is_journaled_and_changes_nothing()
    {
        // The stock service answers with a fact that belongs to the charge.
        Services.Reserve = (_, _) => Task.FromResult<CheckoutFact>(new Charged("stray"));
        var checkout = CheckoutRef("o6");

        await checkout.Start(new PlaceOrder("o6", 100), AskCancellation());
        await SettledIn<Reserving>("o6");

        // The start and the fact.
        Assert.Equal(2, (await HostedState("o6")).SequenceNr);
        Assert.Equal([new ReserveStock("o6")], Services.Issued);
    }

    public static TheoryData<string> StatesWithWork => [nameof(Reserving), nameof(Charging), nameof(Undoing)];

    [Theory]
    [MemberData(nameof(StatesWithWork))]
    public async Task A_restart_with_work_in_flight_does_not_issue_it_again(string state)
    {
        // Run the checkout up to the given state and leave that state's work without an answer.
        switch (state)
        {
            case nameof(Reserving):
                Services.Reserve = (_, ct) => Never(ct);
                break;
            case nameof(Charging):
                Services.Charge = (_, ct) => Never(ct);
                break;
            default:
                Services.Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("no funds"));
                Services.Undo = (_, ct) => Never(ct);
                break;
        }

        var id = $"restart-{state}";
        var checkout = CheckoutRef(id);
        await checkout.Start(new PlaceOrder(id, 100), AskCancellation());
        var before = await AwaitRunning(id, running =>
        {
            Assert.Equal(state, ((CheckoutState)running.State).Value!.GetType().Name);
            Assert.Empty(running.Settled);
        });
        // Each state so far issued one piece of work, so the handlers have seen one effect for each entry.
        await AwaitAssertAsync(() => Assert.Equal(before.Entry, Services.Issued.Count), cancellationToken: TestCancellation);
        var issuedBefore = Services.Issued;

        await RestartWorkflow(id);

        // The next message starts the workflow again. The interrupted work gives up; the table sends it to a person.
        var after = await SettledIn<NeedsAttention>(id);
        var attention = Assert.IsType<NeedsAttention>(((CheckoutState)after.State).Value);
        Assert.Equal(before.State, attention.Was);
        Assert.EndsWith(nameof(GaveUpReason.Interrupted), attention.Why);

        // Only the ticket was issued after the restart.
        Assert.Equal([.. issuedBefore, new OpenSupportTicket(id, attention.Why)], Services.Issued);
    }

    [Fact]
    public async Task A_restart_after_the_work_was_answered_changes_nothing()
    {
        Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        var checkout = CheckoutRef("o7");
        await checkout.Start(new PlaceOrder("o7", 100), AskCancellation());
        await SettledIn<NeedsAttention>("o7");
        var before = await HostedState("o7");
        var issuedBefore = Services.Issued;

        await RestartWorkflow("o7");

        Assert.Equal(before, await HostedState("o7"));
        await Task.Delay(NoChangeWindow, TestCancellation);
        Assert.Equal(before, await HostedState("o7"));
        Assert.Equal(issuedBefore, Services.Issued);
    }

    [Fact]
    public async Task A_finished_workflow_stays_finished_after_a_restart()
    {
        var checkout = CheckoutRef("o8");
        await checkout.Start(new PlaceOrder("o8", 100), AskCancellation());
        await checkout.WaitForOutcome(Poll, AskCancellation());

        await RestartWorkflow("o8");

        var again = Assert.IsType<AlreadyFinished<CheckoutOutcome>>(
            (await checkout.Start(new PlaceOrder("o8", 100), AskCancellation())).Value);
        Assert.Equal(new OrderPlaced("charge-o8"), again.Outcome.Value);
        Assert.Equal(2, Services.Issued.Count);
    }

    [Fact]
    public async Task Two_workflow_types_run_in_one_actor_system()
    {
        var echo = WorkflowRef<Shout, Heard>("e1");
        var checkout = CheckoutRef("e1");

        await echo.Start(new Shout("hello"), AskCancellation());
        await checkout.Start(new PlaceOrder("e1", 100), AskCancellation());

        Assert.Equal(new Heard("hello"), await echo.WaitForOutcome(Poll, AskCancellation()));
        Assert.Equal(new OrderPlaced("charge-e1"), (await checkout.WaitForOutcome(Poll, AskCancellation())).Value);
    }
}

public sealed class LocalCheckoutWorkflowSpecs(ITestOutputHelper output) : CheckoutWorkflowSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedCheckoutWorkflowSpecs(ITestOutputHelper output) : CheckoutWorkflowSpecs(output)
{
    protected override bool Sharded => true;
}
