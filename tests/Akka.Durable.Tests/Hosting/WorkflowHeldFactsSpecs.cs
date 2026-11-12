using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Reminders;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A checkout whose decisions are recorded, and which has a few extra rules so that a held fact can stay, repeat
/// the state's work, or move the workflow on:
/// <list type="bullet">
/// <item>While charging, <c>OperatorSaidRetry</c> repeats the state (<c>Again</c>).</item>
/// <item>While undoing, <c>OperatorSaidRetry</c> changes nothing, and <c>OperatorSaidAbandon</c> sends the checkout to a person.</item>
/// <item>Work that gives up undoing is taken as undone, and a charge that gives up as a charge.</item>
/// </list>
/// Everything else is the standard checkout table.
/// </summary>
internal static class HeldFactsCheckout
{
    public static WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> With(
        ConcurrentQueue<(CheckoutState State, CheckoutFact Fact)> decisions)
    {
        return Checkout.Definition with
        {
            Decide = (state, fact, ctx) =>
            {
                decisions.Enqueue((state, fact));
                return (state, fact) switch
                {
                    (Charging, OperatorSaidRetry) => ctx.Again(),
                    (Undoing, OperatorSaidRetry) => ctx.Stay(),
                    (Undoing, OperatorSaidAbandon) => ctx.Next(new NeedsAttention(state, "abandoned")),
                    _ => Checkout.Decide(state, fact, ctx),
                };
            },
            GaveUp = failure => failure.What switch
            {
                var what when what.StartsWith("UndoOrder", StringComparison.Ordinal) => new Undone(),
                var what when what.StartsWith("ChargeCard", StringComparison.Ordinal) => new Charged("gave-up"),
                _ => new WorkStuck(failure.What, failure.Reason),
            },
        };
    }

    public static string Name(CheckoutFact fact) => fact.Value.GetType().Name;
}

public abstract class WorkflowHeldFactsKit(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    protected readonly ConcurrentQueue<(CheckoutState State, CheckoutFact Fact)> Decisions = new();

    private WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>? _definition;

    protected override WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        _definition ??= HeldFactsCheckout.With(Decisions);

    protected static TaskCompletionSource<CheckoutFact> Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected static Func<T, CancellationToken, Task<CheckoutFact>> Answers<T>(TaskCompletionSource<CheckoutFact> gate) =>
        (_, ct) => gate.Task.WaitAsync(ct);

    protected string[] DecidedFacts() => [.. Decisions.Select(d => HeldFactsCheckout.Name(d.Fact))];

    /// <summary>Waits until the workflow is running in a state that <paramref name="accept"/> takes.</summary>
    protected async Task<WorkflowRunning<CheckoutState>> AwaitRunningIn(string id, Func<CheckoutState, bool> accept)
    {
        WorkflowRunning<CheckoutState>? found = null;
        await AwaitAssertAsync(async () =>
        {
            found = Assert.IsType<WorkflowRunning<CheckoutState>>((await CheckoutRef(id).GetStateAsync(AskCancellation())).State.Value);
            Assert.True(accept(found.State), $"The workflow is in {found.State.Value}.");
        }, cancellationToken: TestCancellation);
        return found!;
    }

    /// <summary>Runs a checkout to a charge that is in flight, and waits until it is issued.</summary>
    protected async Task<IWorkflowRef<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>> ChargingWith(
        string id, TaskCompletionSource<CheckoutFact> charge)
    {
        Services.Charge = Answers<ChargeCard>(charge);
        var checkout = CheckoutRef(id);
        await checkout.Start(new PlaceOrder(id, 100), AskCancellation());
        await AwaitRunningIn(id, s => s.Value is Charging);
        return checkout;
    }

    /// <summary>Runs a checkout whose charge is declined, to an undo that is in flight.</summary>
    protected async Task<IWorkflowRef<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>> UndoingWith(
        string id, TaskCompletionSource<CheckoutFact> undo)
    {
        Services.Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("no funds"));
        Services.Undo = Answers<UndoOrder>(undo);
        var checkout = CheckoutRef(id);
        await checkout.Start(new PlaceOrder(id, 100), AskCancellation());
        await AwaitRunningIn(id, s => s.Value is Undoing);
        return checkout;
    }
}

/// <summary>
/// Facts that are held while work is in flight are decided before the answer's finish takes effect.
/// </summary>
public abstract class WorkflowHeldFactsSpecs(ITestOutputHelper output) : WorkflowHeldFactsKit(output)
{
    [Fact]
    public async Task A_held_fact_that_repeats_the_state_is_decided_before_the_answers_finish_and_the_workflow_runs_on()
    {
        var first = Gate();
        var checkout = await ChargingWith("h1", first);

        Assert.IsType<Held>((await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);
        // The second charge, issued when the held fact repeats the state, waits for its own answer.
        var second = Gate();
        Services.Charge = Answers<ChargeCard>(second);
        first.SetResult(new Charged("first"));

        await AwaitAssertAsync(() => Assert.Equal(2, Services.Issued.OfType<ChargeCard>().Count()), cancellationToken: TestCancellation);
        var running = Assert.IsType<WorkflowRunning<CheckoutState>>((await checkout.GetStateAsync(AskCancellation())).State.Value);
        Assert.IsType<Charging>(running.State.Value);

        second.SetResult(new Charged("second"));
        Assert.Equal(new OrderPlaced("second"), (await checkout.WaitForOutcome(AskCancellation())).Value);
        Assert.Equal(["Charged", "OperatorSaidRetry", "Charged"], DecidedFacts().Skip(1));
    }

    [Fact]
    public async Task A_held_fact_that_moves_the_workflow_on_is_taken_and_the_answers_finish_does_not_happen()
    {
        var undo = Gate();
        var checkout = await UndoingWith("h2", undo);

        Assert.IsType<Held>((await checkout.Notify(new OperatorSaidAbandon(), AskCancellation())).Value);
        undo.SetResult(new Undone());

        var waiting = await AwaitRunningIn("h2", s => s.Value is NeedsAttention);
        var attention = Assert.IsType<NeedsAttention>(waiting.State.Value);
        Assert.Equal("abandoned", attention.Why);
        Assert.IsType<Undoing>(attention.Was.Value);

        // Still running: the finish of the undo did not happen, and the person's ticket is opened.
        await AwaitAssertAsync(() => Assert.Single(Services.Issued.OfType<OpenSupportTicket>()), cancellationToken: TestCancellation);
        Assert.IsType<WorkflowRunning<CheckoutState>>((await checkout.GetStateAsync(AskCancellation())).State.Value);
        var late = await checkout.Notify(new OperatorSaidRetry(), AskCancellation());
        Assert.IsNotType<AlreadyFinished<CheckoutOutcome>>(late.Value);
    }

    [Fact]
    public async Task When_no_held_fact_moves_the_workflow_it_finishes_with_the_answers_outcome_and_each_fact_was_decided_once()
    {
        var undo = Gate();
        var checkout = await UndoingWith("h3", undo);

        Assert.IsType<Held>((await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);
        Assert.IsType<Held>((await checkout.Cancel("customer asked", "user-17", AskCancellation())).Value);
        undo.SetResult(new Undone());

        Assert.Equal(new OrderFailed("no funds"), (await checkout.WaitForOutcome(AskCancellation())).Value);

        // The answer, then the held facts in the order they arrived, each once.
        Assert.Equal(["Declined", "Undone", "OperatorSaidRetry", "CancelRequested"], DecidedFacts().Skip(1));
        Assert.All(Decisions.Skip(3), d => Assert.IsType<Undoing>(d.State.Value));

        // Closed after the finish, with the outcome.
        var closed = Assert.IsType<AlreadyFinished<CheckoutOutcome>>((await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);
        Assert.Equal(new OrderFailed("no funds"), closed.Outcome.Value);
        Assert.Equal(5, Decisions.Count);
    }

    [Fact]
    public async Task Of_two_held_facts_the_first_stays_and_the_second_moves_the_workflow_on()
    {
        var undo = Gate();
        var checkout = await UndoingWith("h4", undo);

        Assert.IsType<Held>((await checkout.Notify(new OperatorSaidRetry(), AskCancellation())).Value);
        Assert.IsType<Held>((await checkout.Notify(new OperatorSaidAbandon(), AskCancellation())).Value);
        undo.SetResult(new Undone());

        var waiting = await AwaitRunningIn("h4", s => s.Value is NeedsAttention);
        Assert.Equal("abandoned", Assert.IsType<NeedsAttention>(waiting.State.Value).Why);
        Assert.Equal(["Declined", "Undone", "OperatorSaidRetry", "OperatorSaidAbandon"], DecidedFacts().Skip(1).Take(4));
        Assert.IsType<WorkflowRunning<CheckoutState>>((await checkout.GetStateAsync(AskCancellation())).State.Value);
    }

    [Fact]
    public async Task A_held_cancel_is_decided_before_a_finish_and_the_workflow_cancels_instead()
    {
        var charge = Gate();
        var undo = Gate();
        Services.Undo = Answers<UndoOrder>(undo);
        var checkout = await ChargingWith("h5", charge);

        Assert.IsType<Held>((await checkout.Cancel("customer asked", "user-17", AskCancellation())).Value);
        charge.SetResult(new Charged("c-h5"));

        // The charge went through, and would have finished the order, but the cancel is decided first.
        await AwaitRunningIn("h5", s => s.Value is Undoing { Then.Value: OrderCancelled });
        Assert.IsType<WorkflowRunning<CheckoutState>>((await checkout.GetStateAsync(AskCancellation())).State.Value);

        undo.SetResult(new Undone());
        Assert.Equal(new OrderCancelled("customer asked"), (await checkout.WaitForOutcome(AskCancellation())).Value);
        Assert.Equal([new ReserveStock("h5"), new ChargeCard("h5", 100), new UndoOrder("h5")], Services.Issued);
    }

    [Fact]
    public async Task A_held_cancel_that_changes_nothing_leaves_the_finish_as_it_was()
    {
        var undo = Gate();
        var checkout = await UndoingWith("h6", undo);

        Assert.IsType<Held>((await checkout.Cancel("customer asked", "user-17", AskCancellation())).Value);
        undo.SetResult(new Undone());

        Assert.Equal(new OrderFailed("no funds"), (await checkout.WaitForOutcome(AskCancellation())).Value);
        Assert.Equal(1, Decisions.Count(d => d.Fact.Value is CancelRequested));
    }

    [Fact]
    public async Task A_workflow_that_holds_nothing_at_its_finish_finishes_as_before()
    {
        var checkout = CheckoutRef("h7");
        await checkout.Start(new PlaceOrder("h7", 100), AskCancellation());

        Assert.Equal(new OrderPlaced("charge-h7"), (await checkout.WaitForOutcome(AskCancellation())).Value);
        Assert.Equal(["Reserved", "Charged"], DecidedFacts());
    }
}

public sealed class LocalWorkflowHeldFactsSpecs(ITestOutputHelper output) : WorkflowHeldFactsSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowHeldFactsSpecs(ITestOutputHelper output) : WorkflowHeldFactsSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>
/// An answer to work of an entry the workflow has left is dropped. It is not decided and not held, even when the
/// state the workflow is in now would accept that kind of fact.
/// </summary>
/// <remarks>
/// Reserving issues two pieces of work together: the reservation and a charge. The reservation answers first, which
/// moves the workflow to Charging, a state with no work of its own. The charge of the earlier entry answers after
/// that, with <c>Charged</c>: the fact that Charging itself finishes on.
/// </remarks>
public sealed class WorkflowLateAnswerSpecs(ITestOutputHelper output) : WorkflowHeldFactsKit(output)
{
    private WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>? _definition;

    protected override WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        _definition ??= HeldFactsCheckout.With(Decisions) with
        {
            Work = (state, ctx) => state switch
            {
                Reserving => ctx.All(ctx.Execute(new ReserveStock(ctx.Input.Order)), ctx.Execute(new ChargeCard(ctx.Input.Order, ctx.Input.Total))),
                Charging => ctx.Nothing(),
                _ => Checkout.Work(state, ctx),
            },
        };

    [Fact]
    public async Task A_late_answer_to_work_of_an_earlier_entry_is_not_decided()
    {
        var slow = Gate();
        var slowReturned = Gate();
        Services.Charge = async (_, _) =>
        {
            var answer = await slow.Task;
            slowReturned.SetResult(answer);
            return answer;
        };
        var checkout = CheckoutRef("l1");
        await checkout.Start(new PlaceOrder("l1", 100), AskCancellation());

        // The reservation answers: the workflow is in Charging, and the charge of the entry it left is still out.
        await AwaitRunningIn("l1", s => s.Value is Charging);

        // The charge answers now. Charging finishes on a Charged, but this one is not the answer to Charging's work.
        slow.SetResult(new Charged("late"));
        await slowReturned.Task.WaitAsync(TestCancellation);
        var running = Assert.IsType<WorkflowRunning<CheckoutState>>((await checkout.GetStateAsync(AskCancellation())).State.Value);
        Assert.IsType<Charging>(running.State.Value);

        // A real Charged, from outside, is decided as usual.
        Assert.IsType<Delivered>((await checkout.Notify(new Charged("real"), AskCancellation())).Value);
        Assert.Equal(new OrderPlaced("real"), (await checkout.WaitForOutcome(AskCancellation())).Value);
        Assert.Equal(["Reserved", "Charged"], DecidedFacts());
    }
}

/// <summary>
/// Held facts and a restart, on a real journal (Akka.Persistence.Sql on SQLite). A new actor system reads what the
/// last one wrote. Work that was in flight when the system stopped gives up as interrupted, and the definition takes
/// that as an answer.
/// </summary>
public sealed class WorkflowHeldFactsSqlSpecs
{
    private const string Id = "w1";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class Rig : IDisposable
    {
        private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-held-{Guid.NewGuid():N}.db");

        public ConcurrentQueue<(CheckoutState State, CheckoutFact Fact)> Decisions { get; } = new();

        public async Task<IHost> StartAsync(CheckoutServices services)
        {
            var definition = HeldFactsCheckout.With(Decisions);
            var host = new HostBuilder()
                .ConfigureServices(collection =>
                {
                    collection.AddDurableEntities();
                    collection.AddSingleton(services)
                        .AddEffectHandler<ReserveStock, CheckoutFact, ReserveStockHandler>()
                        .AddEffectHandler<ChargeCard, CheckoutFact, ChargeCardHandler>()
                        .AddEffectHandler<UndoOrder, CheckoutFact, UndoOrderHandler>()
                        .AddEffectHandler<OpenSupportTicket, CheckoutFact, OpenSupportTicketHandler>();
                    collection.AddAkka("workflow-held-sql", builder => builder
                        .WithSqlPersistence(connectionString: $"Data Source={_databasePath}", providerName: ProviderName.SQLiteMS)
                        .WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)))
                        .WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails)
                        .WithDurableWorkflow(definition));
                })
                .Build();
            await host.StartAsync(Ct);
            return host;
        }

        public void Dispose()
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            if (File.Exists(_databasePath)) File.Delete(_databasePath);
        }
    }

    private static CancellationToken Within(TimeSpan time)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(time);
        return cts.Token;
    }

    private static CancellationToken Soon => Within(TimeSpan.FromSeconds(30));

    private static IWorkflowRef<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Ref(IHost host) =>
        host.Services.GetRequiredService<IWorkflowRefs<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>().Get(new WorkflowId(Id));

    private static async Task AwaitIn(IHost host, Func<CheckoutState, bool> accept)
    {
        while (true)
        {
            var state = (await Ref(host).GetStateAsync(Soon)).State.Value;
            if (state is WorkflowRunning<CheckoutState> running && accept(running.State)) return;
            await Task.Yield();
        }
    }

    /// <summary>Waits until the hosted workflow has the held fact in its journal state.</summary>
    private static async Task AwaitHeld(IHost host)
    {
        var region = host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>>().ActorRef;
        while (true)
        {
            var state = await new EntityRef<WfCommand, WfState, WfEvent>(new EntityId(Id), region, host.Services.GetRequiredService<ActorSystem>()).GetStateAsync(Soon);
            if (state.State.Value is WfRunning { Held.Length: > 0 }) return;
            await Task.Yield();
        }
    }

    private static async Task<CheckoutFact> Hang(CancellationToken ct)
    {
        await Task.Delay(Timeout.Infinite, ct);
        throw new InvalidOperationException("Unreachable: the delay only ends by cancellation.");
    }

    [Fact]
    public async Task A_held_cancel_survives_a_restart_and_is_decided_once_before_the_answers_finish()
    {
        using var rig = new Rig();
        var first = new CheckoutServices { Charge = (_, ct) => Hang(ct) };
        using (var one = await rig.StartAsync(first))
        {
            await Ref(one).Start(new PlaceOrder("w1", 100), Soon);
            await AwaitIn(one, s => s.Value is Charging);
            Assert.IsType<Held>((await Ref(one).Cancel("customer asked", "user-17", Soon)).Value);
            await AwaitHeld(one);
            await one.StopAsync(Ct);
        }

        // The interrupted charge is taken as a charge, which finishes the order, but the held cancel comes first.
        var second = new CheckoutServices();
        using (var two = await rig.StartAsync(second))
        {
            Assert.Equal(new OrderCancelled("customer asked"), (await Ref(two).WaitForOutcome(Soon)).Value);
            Assert.Equal([new UndoOrder("w1")], second.Issued);
            Assert.Equal(1, rig.Decisions.Count(d => d.Fact.Value is CancelRequested));
            await two.StopAsync(Ct);
        }

        // A third system finds the workflow finished and decides and issues nothing.
        var decided = rig.Decisions.Count;
        var third = new CheckoutServices();
        using var last = await rig.StartAsync(third);
        Assert.Equal(new OrderCancelled("customer asked"), (await Ref(last).WaitForOutcome(Soon)).Value);
        Assert.Empty(third.Issued);
        Assert.Equal(decided, rig.Decisions.Count);
        await last.StopAsync(Ct);
    }

    [Fact]
    public async Task Held_facts_that_change_nothing_survive_a_restart_and_the_workflow_finishes_with_the_answers_outcome()
    {
        using var rig = new Rig();
        var first = new CheckoutServices
        {
            Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("no funds")),
            Undo = (_, ct) => Hang(ct),
        };
        using (var one = await rig.StartAsync(first))
        {
            await Ref(one).Start(new PlaceOrder("w1", 100), Soon);
            await AwaitIn(one, s => s.Value is Undoing);
            Assert.IsType<Held>((await Ref(one).Notify(new OperatorSaidRetry(), Soon)).Value);
            Assert.IsType<Held>((await Ref(one).Cancel("customer asked", "user-17", Soon)).Value);
            await one.StopAsync(Ct);
        }

        // The interrupted undo is taken as undone, which finishes the order. Neither held fact moves it on.
        var second = new CheckoutServices();
        using (var two = await rig.StartAsync(second))
        {
            Assert.Equal(new OrderFailed("no funds"), (await Ref(two).WaitForOutcome(Soon)).Value);
            Assert.Empty(second.Issued);
            Assert.Equal(1, rig.Decisions.Count(d => d.Fact.Value is OperatorSaidRetry));
            Assert.Equal(1, rig.Decisions.Count(d => d.Fact.Value is CancelRequested));
            await two.StopAsync(Ct);
        }

        var decided = rig.Decisions.Count;
        var third = new CheckoutServices();
        using var last = await rig.StartAsync(third);
        Assert.Equal(new OrderFailed("no funds"), (await Ref(last).WaitForOutcome(Soon)).Value);
        Assert.Empty(third.Issued);
        Assert.Equal(decided, rig.Decisions.Count);
        await last.StopAsync(Ct);
    }
}
