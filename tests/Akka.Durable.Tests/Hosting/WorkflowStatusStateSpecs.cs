using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Durable.Tests.Fixtures.Echoes;
using Akka.Durable.Tests.Fixtures.Nests;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// What <c>Status</c> reports for a running workflow: the state it is in, typed as the definition types it, and
/// how a ref finds its workflow type by all four of its types.
/// </summary>
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
            .WithDurableWorkflow(Nest.Definition, Sharding())
            .WithDurableWorkflow(Nest.Alone, Sharding())
            .WithDurableWorkflow(Nest.Tagged, Sharding());

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

    // Readers with one arm per case and no catch-all arm. They compile only while State has the union type.
    private static string Read(CheckoutState state) => state switch
    {
        Reserving => "Reserving",
        Charging charging => $"Charging {charging.Reservation}",
        Undoing undoing => "Undoing then " + undoing.Then switch
        {
            OrderPlaced placed => $"placed {placed.Charge}",
            OrderFailed failed => $"failed {failed.Reason}",
            OrderCancelled cancelled => $"cancelled {cancelled.Reason}",
        },
        NeedsAttention attention => $"NeedsAttention {attention.Why} from {Read(attention.Was)}",
    };

    private static string Read(Outer state) => state switch
    {
        Inner inner => inner switch { Low low => $"Low {low.Level}", High => "High" },
        Settled => "Settled",
    };

    private static string Read(Lone state) => state switch { Settled => "Settled" };

    [Fact]
    public async Task Status_holds_the_state_union_with_the_data_the_facts_gave_it_after_each_move()
    {
        var reserved = new TaskCompletionSource<CheckoutFact>(TaskCreationOptions.RunContinuationsAsynchronously);
        Services.Reserve = (_, ct) => reserved.Task.WaitAsync(ct);
        Services.Charge = (_, ct) => Never(ct);
        var checkout = CheckoutRef("s1");
        await checkout.Start(new PlaceOrder("s1", 100), AskCancellation());

        // Work is in flight. Reserving is a case with no fields.
        var first = await Running(checkout, r => Assert.Equal("Reserving", Read(r.State)));
        Assert.Equal(nameof(Reserving), first.StateName);

        reserved.SetResult(new Reserved("res-fed"));
        var second = await Running(checkout, r => Assert.Equal("Charging res-fed", Read(r.State)));
        Assert.Equal(nameof(Charging), second.StateName);
        Assert.True(second.Since >= first.Since);
    }

    [Fact]
    public async Task A_fact_held_while_work_is_in_flight_leaves_the_state_until_it_is_decided()
    {
        var reserved = new TaskCompletionSource<CheckoutFact>(TaskCreationOptions.RunContinuationsAsynchronously);
        Services.Reserve = (_, ct) => reserved.Task.WaitAsync(ct);
        Services.Charge = (_, ct) => Never(ct);
        Services.Undo = (_, ct) => Never(ct);
        var checkout = CheckoutRef("s7");
        await checkout.Start(new PlaceOrder("s7", 100), AskCancellation());

        Assert.IsType<Held>((await checkout.Cancel("customer asked", "user-7", AskCancellation())).Value);
        await Running(checkout, r => Assert.Equal("Reserving", Read(r.State)));

        // The reservation moves the checkout to Charging; the held cancel is decided there and undoes the order.
        reserved.SetResult(new Reserved("res-s7"));
        await Running(checkout, r => Assert.Equal("Undoing then cancelled customer asked", Read(r.State)));
    }

    [Fact]
    public async Task A_restart_keeps_the_journaled_state_and_the_time_it_was_entered()
    {
        // A state with no work in flight, so the restart cannot move the workflow: it is held for a person.
        Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        var checkout = CheckoutRef("s2");
        await checkout.Start(new PlaceOrder("s2", 100), AskCancellation());
        await SettledIn<NeedsAttention>("s2");
        const string expected = "NeedsAttention ChargeCard Failed from Charging res-s2";
        var before = await Running(checkout, r => Assert.Equal(expected, Read(r.State)));

        await RestartWorkflow("s2");

        var after = await Running(checkout, r => Assert.Equal(expected, Read(r.State)));
        Assert.Equal(before.Since, after.Since);
    }

    [Fact]
    public async Task A_state_that_holds_another_state_keeps_it_and_its_time_after_Stay()
    {
        Services.Charge = (_, _) => throw new InvalidOperationException("gateway down");
        var checkout = CheckoutRef("s3");
        await checkout.Start(new PlaceOrder("s3", 100), AskCancellation());

        // Reached by a fact the table answered with Next; the ticket fact that follows is answered with Stay.
        const string expected = "NeedsAttention ChargeCard Failed from Charging res-s3";
        var entered = await Running(checkout, r => Assert.Equal(expected, Read(r.State)));
        await SettledIn<NeedsAttention>("s3");
        var stayed = await Running(checkout, r => Assert.Equal(expected, Read(r.State)));
        Assert.Equal(nameof(NeedsAttention), stayed.StateName);
        Assert.Equal(entered.Since, stayed.Since);
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
        var entered = await Running(checkout, r => Assert.Equal("Undoing then failed no funds", Read(r.State)));

        firstUndo.SetResult(new UndoRefused());
        await AwaitAssertAsync(() => Assert.Equal(2, Services.Issued.OfType<UndoOrder>().Count()), cancellationToken: TestCancellation);

        var again = await Running(checkout, r => Assert.Equal("Undoing then failed no funds", Read(r.State)));
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
    public async Task A_union_inside_a_union_is_the_outer_union_and_is_named_after_the_innermost_record()
    {
        var nest = WorkflowRef<Go, Outer, NestFact, Over>("s6");
        await nest.Start(new Go(7), AskCancellation());

        var running = await Running(nest, r => Assert.Equal("Low 7", Read(r.State)));
        Assert.Equal(nameof(Low), running.StateName);
    }

    [Fact]
    public async Task Workflow_types_that_differ_only_in_their_state_type_each_answer_their_own_refs()
    {
        var outer = WorkflowRef<Go, Outer, NestFact, Over>("s9");
        var alone = WorkflowRef<Go, Lone, NestFact, Over>("s9");
        var tagged = WorkflowRef<Go, Tagged<int>, NestFact, Over>("s9");
        await outer.Start(new Go(1), AskCancellation());
        await alone.Start(new Go(2), AskCancellation());
        await tagged.Start(new Go(3), AskCancellation());

        await Running(outer, r => Assert.Equal("Low 1", Read(r.State)));
        var lone = await Running(alone, r => Assert.Equal("Settled", Read(r.State)));
        Assert.Equal(nameof(Settled), lone.StateName);
        await Running(tagged, r => Assert.Equal(new Tagged<int>(3), r.State));
    }

    public static TheoryData<string> WrongTypes => ["state", "object state", "fact", "outcome"];

    [Theory]
    [MemberData(nameof(WrongTypes))]
    public void A_ref_whose_four_types_match_no_hosted_workflow_type_cannot_be_obtained(string wrong)
    {
        Action get = wrong switch
        {
            "state" => GetFails<PlaceOrder, Outer, CheckoutFact, CheckoutOutcome>,
            "object state" => GetFails<PlaceOrder, object, CheckoutFact, CheckoutOutcome>,
            "fact" => GetFails<PlaceOrder, CheckoutState, NestFact, CheckoutOutcome>,
            "outcome" => GetFails<PlaceOrder, CheckoutState, CheckoutFact, Over>,
            _ => throw new ArgumentOutOfRangeException(nameof(wrong), wrong, null),
        };

        get();
    }

    private void GetFails<TInput, TState, TFact, TOutcome>() where TState : notnull
    {
        var ex = Assert.Throws<InvalidOperationException>(() => WorkflowRef<TInput, TState, TFact, TOutcome>("s10"));

        foreach (var type in new[] { typeof(TInput), typeof(TState), typeof(TFact), typeof(TOutcome) })
            Assert.Contains(type.FullName!, ex.Message);
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
    [InlineData("case that holds a union", nameof(NeedsAttention))]
    [InlineData("union inside a union", nameof(Low))]
    [InlineData("single case with no fields", nameof(Settled))]
    public void StateName_is_the_name_of_the_record_the_state_holds(string kind, string expected)
    {
        var name = kind switch
        {
            "plain" => new WorkflowRunning<Listening>(new Listening(), Entered).StateName,
            "union" => new WorkflowRunning<CheckoutState>(new Charging("r1"), Entered).StateName,
            "case that holds a union" =>
                new WorkflowRunning<CheckoutState>(new NeedsAttention(new Charging("r1"), "why"), Entered).StateName,
            "union inside a union" => new WorkflowRunning<Outer>(new Inner(new Low(1)), Entered).StateName,
            "single case with no fields" => new WorkflowRunning<Lone>(new Settled(), Entered).StateName,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null),
        };

        Assert.Equal(expected, name);
    }

    [Theory]
    [InlineData("a", 0, "a", 0, true)]
    [InlineData("a", 0, "b", 0, false)]
    [InlineData("a", 0, "a", 1, false)]
    public void Two_values_are_equal_when_their_states_and_times_are_equal(
        string reservation, int seconds, string otherReservation, int otherSeconds, bool equal)
    {
        var one = new WorkflowRunning<CheckoutState>(new Charging(reservation), Entered.AddSeconds(seconds));
        var other = new WorkflowRunning<CheckoutState>(new Charging(otherReservation), Entered.AddSeconds(otherSeconds));

        Assert.Equal(equal, one.Equals(other));
        if (equal) Assert.Equal(one.GetHashCode(), other.GetHashCode());
    }
}
