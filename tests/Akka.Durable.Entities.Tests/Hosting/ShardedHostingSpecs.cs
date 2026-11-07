using Akka.Cluster;
using Akka.Cluster.Hosting;
using Akka.Durable.Entities.Tests.Fixtures;
using Akka.Hosting;
using Akka.Remote.Hosting;
using Xunit;

namespace Akka.Durable.Entities.Tests.Hosting;

/// <summary>
/// The same entity definition hosted in a Cluster.Sharding region on a single-node cluster.
/// </summary>
public class ShardedHostingSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    private static readonly TimeSpan ClusterFormation = TimeSpan.FromSeconds(30);

    protected override ShardOptions? Sharding => new();

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder)
    {
        builder
            .WithRemoting("localhost", 0)
            .WithClustering()
            .AddStartup((system, _) =>
            {
                var cluster = Cluster.Cluster.Get(system);
                cluster.Join(cluster.SelfAddress);
            });
    }

    // The first message waits for the cluster and shard coordinator to come up.
    private Task<T> FirstAsk<T>(Func<CancellationToken, Task<T>> ask) => ask(AskCancellation(ClusterFormation));

    [Fact]
    public async Task Entity_ref_round_trips_through_the_shard_region()
    {
        var counter = CounterRef("a");

        await FirstAsk(ct => counter.Ask<Accepted>(new Increment(2), ct));

        Assert.Equal(2, await CountOf(counter));
    }

    [Fact]
    public async Task Stop_passivates_through_the_shard_and_the_entity_recovers_on_next_use()
    {
        var counter = CounterRef("a");
        await FirstAsk(ct => counter.Ask<Accepted>(new Increment(4), ct));

        await counter.Ask<Accepted>(new Shutdown(), AskCancellation());

        Assert.Equal(4, await CountOf(counter));
    }

    [Fact]
    public async Task Using_an_entity_immediately_after_it_stops_loses_no_messages()
    {
        var counter = CounterRef("a");
        await FirstAsk(ct => counter.Ask<CountIs>(new GetCount(), ct));

        await AssertStopThenUseLosesNothing(counter, rounds: 10);
    }

    [Fact]
    public async Task Reminders_are_delivered_to_sharded_entities()
    {
        var counter = CounterRef("a");
        await FirstAsk(ct => counter.Ask<CountIs>(new GetCount(), ct));

        counter.Tell(new RemindLater(TimeSpan.FromMilliseconds(200)));

        await AwaitAssertAsync(
            async () => Assert.Equal(1, await CountOf(counter)),
            duration: TimeSpan.FromSeconds(15),
            interval: TimeSpan.FromMilliseconds(250),
            cancellationToken: TestCancellation);
    }
}
