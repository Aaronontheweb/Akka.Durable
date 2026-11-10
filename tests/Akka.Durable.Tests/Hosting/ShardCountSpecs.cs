using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// <see cref="EntityOptions.NumberOfShards"/> is the shard count sharding actually uses.
/// </summary>
public class ShardCountSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    private const int Shards = 3;

    protected override ShardedHosting? Sharding => new();

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        Counter.Definition with { Options = EntityOptions.Default with { NumberOfShards = Shards } };

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder) => ConfigureSingleNodeCluster(builder);

    [Fact]
    public async Task Entities_are_spread_across_the_configured_number_of_shards()
    {
        var ids = Enumerable.Range(0, 20).Select(i => $"entity-{i}").ToList();

        // The first message waits for the cluster and shard coordinator to come up.
        await CounterRef(ids[0]).Ask(new Increment(1), AskCancellation(TimeSpan.FromSeconds(30)));
        foreach (var id in ids.Skip(1))
            await CounterRef(id).Ask(new Increment(1), AskCancellation());

        var region = Host.Services.GetRequiredService<IRequiredActor<EntityRegion<CounterCommand>>>().ActorRef;
        var state = await region.Ask<CurrentShardRegionState>(GetShardRegionState.Instance, AskCancellation());

        Assert.Empty(state.Failed);
        Assert.All(state.Shards, shard => Assert.InRange(int.Parse(shard.ShardId), 0, Shards - 1));
        Assert.Equal(ids.Order(), state.Shards.SelectMany(s => s.EntityIds).Order());
    }
}
