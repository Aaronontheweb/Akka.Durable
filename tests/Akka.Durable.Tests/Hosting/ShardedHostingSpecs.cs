using Akka.Cluster.Hosting;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The same entity definition hosted in a Cluster.Sharding region on a single-node cluster.
/// </summary>
public class ShardedHostingSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    private static readonly TimeSpan ClusterFormation = TimeSpan.FromSeconds(30);

    protected override ShardOptions? Sharding => new();

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder) => ConfigureSingleNodeCluster(builder);

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

    [Fact]
    public async Task GetState_returns_the_committed_state_and_sequence_number()
    {
        var counter = DurableCounterRef("a");
        await FirstAsk(ct => counter.Ask<CountIs>(new GetCount(), ct));

        await AssertGetStateReturnsCommittedStateAndSequenceNr(counter);
    }

    [Fact]
    public async Task GetState_of_a_fresh_entity_returns_its_initial_state()
    {
        var snapshot = await FirstAsk(ct => DurableCounterRef("fresh").GetStateAsync(ct));

        Assert.Equal(new CounterState(0, false), snapshot.State);
        Assert.Equal(0, snapshot.SequenceNr);
    }

    [Fact]
    public async Task GetState_waits_for_in_flight_effects()
    {
        var counter = DurableCounterRef("a");
        await FirstAsk(ct => counter.Ask<CountIs>(new GetCount(), ct));

        await AssertGetStateWaitsForInFlightEffects(counter);
    }

    [Fact]
    public async Task A_plain_ref_can_be_upgraded_by_id_to_read_state()
    {
        var plain = CounterRef("a");
        await FirstAsk(ct => plain.Ask<Accepted>(new Increment(3), ct));

        var snapshot = await DurableEntityRefs<CounterCommand, CounterState, CounterEvent>().Get(plain.Id)
            .GetStateAsync(AskCancellation());

        Assert.Equal(3, snapshot.State.Count);
    }

    [Fact]
    public void A_state_type_that_does_not_match_the_definition_is_rejected() =>
        AssertWrongStateTypeIsRejected();
}
