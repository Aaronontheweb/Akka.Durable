using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A host stops an entity it thinks is idle. An entity with an <c>Execute</c> handler in flight is not idle:
/// it stops once it has decided on the handler's outcome.
/// </summary>
public abstract class StopWhileExecuteRunsSpec(ITestOutputHelper output) : CounterHostingSpec(output)
{
    // Long enough that a CI stall between keep-alive messages can't look like idleness,
    // short enough to keep the tests quick.
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(1);

    // The first message waits for the cluster and shard coordinator to come up in sharded mode.
    private static readonly TimeSpan FirstAskTimeout = TimeSpan.FromSeconds(30);

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        Counter.Definition with { Options = EntityOptions.Default with { PassivateIdleAfter = IdleTimeout } };

    /// <summary>Actor path of the entity with the given id.</summary>
    protected abstract string EntityPath(string id);

    private Task<IActorRef> EntityActorOf(string id) =>
        Sys.ActorSelection(EntityPath(id)).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);

    [Fact]
    public async Task An_idle_entity_is_not_stopped_while_an_Execute_handler_runs()
    {
        var counter = CounterRef("a");
        await counter.Ask(new GetCount(), AskCancellation(FirstAskTimeout));
        var entity = await EntityActorOf("a");
        Watch(entity);

        // The handler waits for the gate. The reply follows once its outcome is journaled.
        counter.Tell(new IncrementViaService(5, Wait: true), TestActor);

        // Several idle timeouts: the host asks the entity to stop, and the entity stays up.
        await ExpectNoMsgAsync(IdleTimeout * 3, TestCancellation);

        // The host buffers this until the entity stops, then starts a new incarnation for it.
        counter.Tell(new GetCount(), TestActor);
        Gate.Open();

        // Terminated and the replies come from different senders, so don't depend on their order.
        var received = await ReceiveNAsync(3, cancellationToken: TestCancellation).ToListAsync(TestCancellation);
        Assert.Single(received.OfType<Accepted>());
        Assert.Contains(received.OfType<Terminated>(), t => t.ActorRef.Equals(entity));

        // The new incarnation recovered the outcome from the journal.
        Assert.Equal(10, Assert.Single(received.OfType<CountIs>()).Count);
        Assert.NotEqual(entity, await EntityActorOf("a"));
    }

    [Fact]
    public async Task A_stop_with_nothing_in_flight_stops_the_entity_at_once()
    {
        // An Execute handler has run and its outcome is decided: nothing is in flight.
        var counter = CounterRef("a");
        await counter.Ask(new IncrementViaService(1, Wait: false), AskCancellation(FirstAskTimeout));
        var entity = await EntityActorOf("a");
        Watch(entity);

        // Sent past the host, right behind the stop. An entity that stops at once never answers it.
        entity.Tell(StopEntity.Instance);
        entity.Tell(new GetCount().ToCommand(), TestActor);

        await ExpectTerminatedAsync(entity, cancellationToken: TestCancellation);
        await ExpectNoMsgAsync(NoMsgWindow, TestCancellation);
        Assert.Equal(2, await CountOf(counter));
    }
}

public class LocalStopWhileExecuteRunsSpecs(ITestOutputHelper output) : StopWhileExecuteRunsSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    protected override string EntityPath(string id) =>
        $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/{Uri.EscapeDataString(id)}";
}

public class ShardedStopWhileExecuteRunsSpecs(ITestOutputHelper output) : StopWhileExecuteRunsSpec(output)
{
    protected override ShardedHosting? Sharding => new();

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder) => ConfigureSingleNodeCluster(builder);

    protected override string EntityPath(string id) => $"/system/sharding/{Counter.TypeName.Value}/*/{id}";
}

/// <summary>
/// One <see cref="ShardedHosting"/> value reused for two entity types. Every entity type has the same stop
/// message, and each registration gets its own sharding settings.
/// </summary>
public class SharedShardedHostingSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(1);

    // The shard checks every IdleTimeout / 2, so an idle entity stops within 1.5x the timeout.
    private static readonly TimeSpan PassivationBound = TimeSpan.FromSeconds(15);

    private static readonly TimeSpan ClusterFormation = TimeSpan.FromSeconds(30);

    private static readonly EntityOptions Idle = EntityOptions.Default with { PassivateIdleAfter = IdleTimeout };

    private readonly ShardedHosting _shared = new();

    protected override ShardedHosting? Sharding => _shared;

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        Counter.Definition with { Options = Idle };

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder) => ConfigureSingleNodeCluster(builder);

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureEntities(builder, provider);
        builder.WithCustomSerializer(TallySerializer.CreateSerializerDetails)
            .WithDurableEntity(Tally.Definition with { Options = Idle }, _shared);
    }

    private Task<IActorRef> EntityActorOf(EntityTypeName type, string id) =>
        Sys.ActorSelection($"/system/sharding/{type.Value}/*/{id}").ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);

    [Fact]
    public async Task Two_entity_types_hosted_with_one_ShardedHosting_both_passivate()
    {
        await CounterRef("a").Ask(new GetCount(), AskCancellation(ClusterFormation));
        await DurableEntityRef<TallyCommand, TallyState, TallyEvent>("t").GetStateAsync(AskCancellation(ClusterFormation));
        var counter = await EntityActorOf(Counter.TypeName, "a");
        var tally = await EntityActorOf(Tally.TypeName, "t");
        Watch(counter);
        Watch(tally);

        // Either may stop first.
        var stopped = await ReceiveNAsync(2, PassivationBound, TestCancellation).ToListAsync(TestCancellation);
        Assert.Contains(stopped.OfType<Terminated>(), t => t.ActorRef.Equals(counter));
        Assert.Contains(stopped.OfType<Terminated>(), t => t.ActorRef.Equals(tally));
    }
}
