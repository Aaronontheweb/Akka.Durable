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
        services.AddEffectHandler<Calculate, RailwayCommand, CalculateHandler>();
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
            .WithDurableEntity(Railway.Definition, Sharded ? new ShardedHosting() : null);
    }

    private Calculator Calculator => Host.Services.GetRequiredService<Calculator>();
    private EffectRecorder Recorder => Host.Services.GetRequiredService<EffectRecorder>();
    private IDurableEntityRef<RailwayCommand, RailwayState, RailwayEvent> Ref =>
        DurableEntityRef<RailwayCommand, RailwayState, RailwayEvent>("a");

    [Fact]
    public async Task Outcome_selects_a_reply_and_is_committed_before_the_reply()
    {
        Calculator.Calculate = (value, _) => Task.FromResult(value * 2);

        AdjustmentReply reply = await Ref.Ask(new Adjust(3), AskCancellation());

        Assert.Equal(new Adjusted(6), Assert.IsType<Adjusted>(reply.Value));
        var state = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(6, state.State.Count);
        Assert.Empty(await PendingEffects<RailwayCommand>("a")); // the answer was journaled
        Assert.Equal(["calculate:3", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Business_rejection_is_a_successful_invocation_with_no_success_events()
    {
        Calculator.Calculate = (_, _) => Task.FromResult(-1);

        var reply = await Ref.Ask(new Adjust(3), AskCancellation());

        Assert.Equal("negative result", Assert.IsType<AdjustmentRejected>(reply.Value).Reason);
        Assert.Equal(0, (await Ref.GetStateAsync(AskCancellation())).State.Count);
        Assert.Empty(await PendingEffects<RailwayCommand>("a"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Synchronous_and_asynchronous_failures_become_the_failure_outcome(bool synchronous)
    {
        Calculator.Calculate = (_, _) => synchronous
            ? throw new InvalidOperationException("service failed")
            : Task.FromException<int>(new InvalidOperationException("service failed"));

        var reply = await Ref.Ask(new Adjust(3), AskCancellation());

        Assert.Equal("service failed", Assert.IsType<AdjustmentRejected>(reply.Value).Reason);
        var state = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(0, state.State.Count);
        Assert.Empty(await PendingEffects<RailwayCommand>("a"));
        Assert.Equal(["calculate:3", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Outcome_is_decided_against_the_state_after_the_initial_commit()
    {
        var reply = await Ref.Ask(new BeginAdjust(10, 3), AskCancellation());

        Assert.Equal(13, Assert.IsType<Adjusted>(reply.Value).Count);
        var state = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(13, state.State.Count);
        Assert.Empty(await PendingEffects<RailwayCommand>("a"));
    }

    [Fact]
    public async Task Failure_does_not_undo_events_committed_with_the_request()
    {
        Calculator.Calculate = (_, _) => Task.FromException<int>(new InvalidOperationException("service failed"));

        var reply = await Ref.Ask(new BeginAdjust(10, 3), AskCancellation());

        Assert.IsType<AdjustmentRejected>(reply.Value);
        var state = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(10, state.State.Count);
        Assert.Empty(await PendingEffects<RailwayCommand>("a")); // the answer was journaled
    }

    [Fact]
    public async Task State_reads_are_not_held_while_a_handler_runs()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var result = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        Calculator.Calculate = (_, _) =>
        {
            entered.TrySetResult();
            return result.Task;
        };

        var reply = Ref.Ask(new Adjust(2), AskCancellation());
        await entered.Task.WaitAsync(AskCancellation());

        var read = await Ref.GetStateAsync(AskCancellation());
        Assert.Equal(0, read.State.Count);
        Assert.False(reply.IsCompleted);

        result.SetResult(4);
        Assert.Equal(4, Assert.IsType<Adjusted>((await reply).Value).Count);
        Assert.Equal(["calculate:2", "disposed"], Recorder.Entries);
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
