using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Durable.Tests.Fixtures.Echoes;
using Akka.Durable.Tests.Fixtures.Nests;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>What <c>Status</c> reports for a running workflow: the state it is in, with its data, not only its name.</summary>
public abstract class WorkflowStatusStateSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    /// <summary>An echo whose one piece of work never answers: a workflow whose state is a plain record.</summary>
    private sealed class HoldEcho : IEffectHandler<Repeat, EchoFact>
    {
        public async Task<EchoFact> HandleAsync(Repeat effect, CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            throw new InvalidOperationException("Unreachable: the delay only ends by cancellation.");
        }
    }

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddCheckoutHandlers()
            .AddScoped<IEffectHandler<Repeat, EchoFact>, HoldEcho>()
            .AddScoped<IEffectHandler<Hold, NestFact>, HoldHandler>();

    protected override void ConfigureMore(AkkaConfigurationBuilder builder) =>
        builder
            .WithCustomSerializer(EchoSerializer.CreateSerializerDetails)
            .WithCustomSerializer(NestSerializer.CreateSerializerDetails)
            .WithDurableWorkflow(Echo.Definition, Sharding())
            .WithDurableWorkflow(Nest.Definition, Sharding());

    private async Task<WorkflowRunning<TState>> Running<TInput, TState, TFact, TOutcome>(
        IWorkflowRef<TInput, TState, TFact, TOutcome> workflow, Action<WorkflowRunning<TState>> assert)
        where TState : notnull
    {
        WorkflowRunning<TState>? found = null;
        await AwaitAssertAsync(async () =>
        {
            found = Assert.IsType<WorkflowRunning<TState>>((await workflow.Status(AskCancellation())).Value);
            assert(found);
        }, cancellationToken: TestCancellation);
        return found!;
    }

    [Fact]
    public async Task Status_is_the_case_with_the_data_the_facts_gave_it_after_each_move()
    {
        var reserved = new TaskCompletionSource<CheckoutFact>(TaskCreationOptions.RunContinuationsAsynchronously);
        Services.Reserve = (_, ct) => reserved.Task.WaitAsync(ct);
        Services.Charge = (_, ct) => Never(ct);
        var checkout = CheckoutRef("s1");
        await checkout.Start(new PlaceOrder("s1", 100), AskCancellation());

        // Work is in flight: the state is the case, not the union that holds it.
        var first = await Running(checkout, r => Assert.Equal(new Reserving(), r.State));
        Assert.Equal(nameof(Reserving), first.StateName);

        reserved.SetResult(new Reserved("res-fed"));
        var second = await Running(checkout, r => Assert.Equal(new Charging("res-fed"), r.State));
        Assert.Equal(nameof(Charging), second.StateName);
        Assert.True(second.Since >= first.Since);

        var status = (await checkout.Status(AskCancellation())).Value;
        Assert.True(status is WorkflowRunning<CheckoutState> { State: Charging { Reservation: "res-fed" } });
    }

    [Fact]
    public async Task A_restart_keeps_the_journaled_state_and_the_time_it_was_entered()
    {
        // A state with no work in flight, so the restart cannot move the workflow: it is held for a person.
        Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        var checkout = CheckoutRef("s2");
        await checkout.Start(new PlaceOrder("s2", 100), AskCancellation());
        await SettledIn<NeedsAttention>("s2");
        var expected = new NeedsAttention(new Charging("res-s2"), "ChargeCard Failed");
        var before = await Running(checkout, r => Assert.Equal(expected, r.State));

        await RestartWorkflow("s2");

        var after = await Running(checkout, r => Assert.Equal(expected, r.State));
        Assert.Equal(before.Since, after.Since);
    }

    [Fact]
    public async Task A_state_that_holds_another_state_reports_the_outer_case_with_the_inner_one_intact()
    {
        Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        var checkout = CheckoutRef("s3");
        await checkout.Start(new PlaceOrder("s3", 100), AskCancellation());

        // Reached by a fact the table answered with Next; the ticket fact that follows is answered with Stay.
        var expected = new NeedsAttention(new Charging("res-s3"), "ChargeCard Failed");
        await Running(checkout, r => Assert.Equal(expected, r.State));
        await SettledIn<NeedsAttention>("s3");
        var running = await Running(checkout, r => Assert.Equal(expected, r.State));
        Assert.Equal(nameof(NeedsAttention), running.StateName);
    }

    [Fact]
    public async Task A_state_the_workflow_repeats_keeps_its_data_and_the_time_it_was_entered()
    {
        var undos = 0;
        var firstUndo = new TaskCompletionSource<CheckoutFact>(TaskCreationOptions.RunContinuationsAsynchronously);
        Services.Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("no funds"));
        Services.Undo = (_, ct) => Interlocked.Increment(ref undos) == 1 ? firstUndo.Task.WaitAsync(ct) : Never(ct);
        var checkout = CheckoutRef("s4");
        await checkout.Start(new PlaceOrder("s4", 100), AskCancellation());
        var expected = new Undoing(new OrderFailed("no funds"));
        var entered = await Running(checkout, r => Assert.Equal(expected, r.State));

        firstUndo.SetResult(new UndoRefused());
        await AwaitAssertAsync(() => Assert.Equal(2, Services.Issued.OfType<UndoOrder>().Count()), cancellationToken: TestCancellation);

        var again = await Running(checkout, r => Assert.Equal(expected, r.State));
        Assert.Equal(entered.Since, again.Since);
    }

    [Fact]
    public async Task A_state_that_is_a_plain_record_is_that_record()
    {
        var echo = WorkflowRef<Shout, Listening, EchoFact, Heard>("s5");
        await echo.Start(new Shout("hello"), AskCancellation());

        var running = await Running(echo, r => Assert.Equal(new Listening(), r.State));
        Assert.Equal(nameof(Listening), running.StateName);
    }

    [Fact]
    public async Task A_union_inside_a_union_reports_the_innermost_case()
    {
        var nest = WorkflowRef<Go, Outer, NestFact, Over>("s6");
        await nest.Start(new Go(7), AskCancellation());

        var running = await Running(nest, r => Assert.NotNull(r.State.Value));
        Assert.Equal<object>(new Low(7), running.State);
        Assert.Equal(nameof(Low), running.StateName);
    }
}

public sealed class LocalWorkflowStatusStateSpecs(ITestOutputHelper output) : WorkflowStatusStateSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedWorkflowStatusStateSpecs(ITestOutputHelper output) : WorkflowStatusStateSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>The value type itself, built by hand: its name and its equality.</summary>
public sealed class WorkflowRunningValueSpecs
{
    private static readonly DateTimeOffset Entered = DateTimeOffset.UnixEpoch.AddHours(1);

    [Theory]
    [InlineData("plain", nameof(Listening))]
    [InlineData("union", nameof(Charging))]
    [InlineData("nested", nameof(Low))]
    public void StateName_is_the_name_of_the_case_the_state_holds(string kind, string expected)
    {
        object state = kind switch
        {
            "plain" => new Listening(),
            "union" => new CheckoutState(new Charging("r1")),
            _ => new Outer(new Inner(new Low(1))),
        };

        Assert.Equal(expected, new WorkflowRunning<object>(state, Entered).StateName);
    }

    [Theory]
    [InlineData("a", 0, "a", 0, true)]
    [InlineData("a", 0, "b", 0, false)]
    [InlineData("a", 0, "a", 1, false)]
    public void Two_values_are_equal_when_their_states_and_times_are_equal(
        string reservation, int seconds, string otherReservation, int otherSeconds, bool equal)
    {
        var one = new WorkflowRunning<Charging>(new Charging(reservation), Entered.AddSeconds(seconds));
        var other = new WorkflowRunning<Charging>(new Charging(otherReservation), Entered.AddSeconds(otherSeconds));

        Assert.Equal(equal, one.Equals(other));
    }
}
