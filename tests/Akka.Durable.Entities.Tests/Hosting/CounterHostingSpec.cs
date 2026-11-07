using Akka.Cluster.Hosting;
using Akka.Durable.Abstractions.Entities;
using Akka.Durable.Entities.Hosting;
using Akka.Durable.Entities.Tests.Fixtures;
using Akka.Durable.TestKit;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Entities.Tests.Hosting;

/// <summary>
/// Hosts the Counter entity through the public hosting API, the way an application would.
/// Subclasses choose the hosting mode.
/// </summary>
public abstract class CounterHostingSpec(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected static readonly TimeSpan NoMsgWindow = TimeSpan.FromMilliseconds(300);

    /// <summary>Shard options for sharded hosting; <c>null</c> hosts locally.</summary>
    protected abstract ShardOptions? Sharding { get; }

    protected virtual void ConfigureCluster(AkkaConfigurationBuilder builder)
    {
    }

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
    }

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        ConfigureCluster(builder);
        builder.WithDurableEntity(Counter.Definition, Sharding);
    }

    protected IEntityRef<CounterCommand> CounterRef(string id) => EntityRef<CounterCommand>(id);

    protected IDurableEntityRef<CounterCommand, CounterState> DurableCounterRef(string id) =>
        DurableEntityRef<CounterCommand, CounterState>(id);

    protected EffectRecorder Recorder => Host.Services.GetRequiredService<EffectRecorder>();

    protected Gate Gate => Host.Services.GetRequiredService<Gate>();

    protected async Task<int> CountOf(IEntityRef<CounterCommand> counter) =>
        (await counter.Ask<CountIs>(new GetCount(), AskCancellation())).Count;

    /// <summary>
    /// The reply to Shutdown is sent before the entity passivates, so the next command races the stop.
    /// The region / host must buffer it for the next incarnation rather than lose it.
    /// </summary>
    protected async Task AssertStopThenUseLosesNothing(IEntityRef<CounterCommand> counter, int rounds)
    {
        for (var i = 1; i <= rounds; i++)
        {
            await counter.Ask<Accepted>(new Increment(1), AskCancellation());
            await counter.Ask<Accepted>(new Shutdown(), AskCancellation());
            Assert.Equal(i, await CountOf(counter));
        }
    }

    protected async Task AssertGetStateReturnsCommittedStateAndSequenceNr(
        IDurableEntityRef<CounterCommand, CounterState> counter)
    {
        await counter.Ask<Accepted>(new Increment(2), AskCancellation());
        await counter.Ask<Accepted>(new LockCounter(), AskCancellation());
        await counter.Ask<Accepted>(new UnlockCounter(), AskCancellation());

        var snapshot = await counter.GetStateAsync(AskCancellation());

        Assert.Equal(new CounterState(2, false), snapshot.State);
        Assert.Equal(3, snapshot.SequenceNr);
    }

    /// <summary>
    /// WaitForGate persists nothing and holds its Run effect open, so a state read sent after it must
    /// wait until the effect finishes.
    /// </summary>
    protected async Task AssertGetStateWaitsForInFlightEffects(IDurableEntityRef<CounterCommand, CounterState> counter)
    {
        var opened = counter.Ask<GateOpened>(new WaitForGate(), AskCancellation());
        var read = counter.GetStateAsync(AskCancellation());

        await Task.Delay(NoMsgWindow, TestCancellation);
        Assert.False(read.IsCompleted, "GetState completed while a Run effect was still in flight");

        Gate.Open();
        await opened;
        Assert.Equal(new CounterState(0, false), (await read).State);
    }

    protected void AssertWrongStateTypeIsRejected()
    {
        var refs = DurableEntityRefs<CounterCommand, string>();

        Assert.Throws<MissingActorRegistryEntryException>(() => refs.Get(new EntityId("a")));
    }
}
