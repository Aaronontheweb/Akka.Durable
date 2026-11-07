using Akka.Cluster.Hosting;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

public abstract class RailwayHostingSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected abstract bool Sharded { get; }

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<Calculator>();
        services.AddSingleton<EffectRecorder>();
        services.AddScoped<ScopedCalculator>();
    }

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        if (Sharded)
            builder.WithRemoting("localhost", 0).WithClustering().AddStartup((system, _) =>
            {
                var cluster = Akka.Cluster.Cluster.Get(system);
                cluster.Join(cluster.SelfAddress);
            });

        builder.WithCustomSerializer(RailwaySerializer.CreateSerializerDetails)
            .WithDurableEntity(Railway.Definition, Sharded ? new ShardOptions() : null);
    }

    private Calculator Calculator => Host.Services.GetRequiredService<Calculator>();
    private EffectRecorder Recorder => Host.Services.GetRequiredService<EffectRecorder>();
    private IDurableEntityRef<RailwayCommand, RailwayState, RailwayEvent> Ref =>
        DurableEntityRef<RailwayCommand, RailwayState, RailwayEvent>("a");

    [Fact]
    public async Task Operation_result_selects_a_reply_and_is_committed_before_the_reply()
    {
        Calculator.Calculate = (value, _) => Task.FromResult(value * 2);

        AdjustmentReply reply = await Ref.Ask(new Adjust(3), AskCancellation());

        Assert.Equal(new Adjusted(6), Assert.IsType<Adjusted>(reply.Value));
        var state = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(6, state.State.Count);
        Assert.Equal(1, state.SequenceNr);
        Assert.Equal(["calculate:3", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Business_rejection_is_a_successful_invocation_with_no_success_events()
    {
        Calculator.Calculate = (_, _) => Task.FromResult(-1);

        var reply = await Ref.Ask(new Adjust(3), AskCancellation());

        Assert.Equal("negative result", Assert.IsType<AdjustmentRejected>(reply.Value).Reason);
        Assert.Equal(0, (await Ref.GetStateAsync(AskCancellation())).SequenceNr);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Synchronous_and_asynchronous_failures_select_only_the_failure_branch(bool synchronous)
    {
        Calculator.Calculate = (_, _) => synchronous
            ? throw new InvalidOperationException("service failed")
            : Task.FromException<int>(new InvalidOperationException("service failed"));

        var reply = await Ref.Ask(new Adjust(3), AskCancellation());

        Assert.Equal("service failed", Assert.IsType<AdjustmentRejected>(reply.Value).Reason);
        var state = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(0, state.State.Count);
        Assert.Equal(0, state.SequenceNr);
        Assert.Equal(["calculate:3", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Continuation_receives_state_after_the_initial_commit_without_repeating_its_events()
    {
        var reply = await Ref.Ask(new BeginAdjust(10, 3), AskCancellation());

        Assert.Equal(13, Assert.IsType<Adjusted>(reply.Value).Count);
        var state = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(13, state.State.Count);
        Assert.Equal(2, state.SequenceNr);
    }

    [Fact]
    public async Task Failure_does_not_undo_events_committed_before_the_operation()
    {
        Calculator.Calculate = (_, _) => Task.FromException<int>(new InvalidOperationException("service failed"));

        var reply = await Ref.Ask(new BeginAdjust(10, 3), AskCancellation());

        Assert.IsType<AdjustmentRejected>(reply.Value);
        var state = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(10, state.State.Count);
        Assert.Equal(1, state.SequenceNr);
    }

    [Fact]
    public async Task Task_without_a_result_uses_a_parameterless_success_continuation()
    {
        var reply = await Ref.Ask(new TaskOnly(), AskCancellation());

        Assert.Equal(0, Assert.IsType<Adjusted>(reply.Value).Count);
        Assert.Equal(["wait", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Task_without_a_result_selects_its_failure_continuation()
    {
        Calculator.Wait = _ => Task.FromException(new InvalidOperationException("service failed"));

        var reply = await Ref.Ask(new TaskOnly(), AskCancellation());

        Assert.Equal(nameof(InvalidOperationException), Assert.IsType<AdjustmentRejected>(reply.Value).Reason);
        Assert.Equal(0, (await Ref.GetStateAsync(AskCancellation())).SequenceNr);
    }

    [Fact]
    public async Task Nested_operations_keep_the_original_reply_type_and_hold_state_queries_until_completion()
    {
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstResult = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondResult = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        Calculator.Calculate = (_, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                firstEntered.SetResult();
                return firstResult.Task;
            }
            secondEntered.SetResult();
            return secondResult.Task;
        };

        var reply = Ref.Ask(new DoubleAdjust(2), AskCancellation());
        await firstEntered.Task.WaitAsync(AskCancellation());
        var read = Ref.GetStateAsync(AskCancellation());
        firstResult.SetResult(4);
        await secondEntered.Task.WaitAsync(AskCancellation());
        Assert.False(reply.IsCompleted);
        Assert.False(read.IsCompleted);
        secondResult.SetResult(8);

        Assert.Equal(8, Assert.IsType<Adjusted>((await reply).Value).Count);
        Assert.Equal(8, (await read).State.Count);
        Assert.Equal(["calculate:2", "disposed", "calculate:4", "disposed"], Recorder.Entries);
    }
}

public sealed class LocalRailwayHostingSpecs(ITestOutputHelper output) : RailwayHostingSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedRailwayHostingSpecs(ITestOutputHelper output) : RailwayHostingSpecs(output)
{
    protected override bool Sharded => true;
}
