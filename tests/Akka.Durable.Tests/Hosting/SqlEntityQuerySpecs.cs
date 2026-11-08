using Akka;
using Akka.Actor;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using LinqToDB;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

public sealed class SqlEntityQuerySpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-queries-{Guid.NewGuid():N}.db");
    private int _initializations;

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        var definition = Counter.Definition with
        {
            InitialState = id =>
            {
                Interlocked.Increment(ref _initializations);
                return Counter.Definition.InitialState(id);
            },
            Options = EntityOptions.Default with
            {
                SnapshotEvery = 2,
                PersistenceIdFactory = (id, type) => $"query::{type.Value}::{id.Value}",
                PassivateIdleAfter = TimeSpan.Zero
            }
        };
        builder.WithSqlPersistence(connectionString: $"Data Source={_databasePath}", providerName: ProviderName.SQLiteMS)
            .AddHocon("akka.persistence.query.journal.sql.refresh-interval = 20ms", HoconAddMode.Prepend)
            .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
            .WithDurableEntity(definition)
            .WithDurableEntityQueries(definition, new EntityQueryOptions("akka.persistence.query.journal.sql"));
    }

    private IDurableEntityRef<CounterCommand, CounterState, CounterEvent> CounterRef(string id = "a") =>
        DurableEntityRef<CounterCommand, CounterState, CounterEvent>(id);

    private async Task Populate()
    {
        var counter = CounterRef();
        await counter.Ask(new Increment(1), AskCancellation());
        await counter.Ask(new Increment(2), AskCancellation());
        await counter.Ask(new Increment(4), AskCancellation());
        await AwaitAssertAsync(() =>
        {
            using var connection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={_databasePath}");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM snapshot";
            Assert.Equal(1L, command.ExecuteScalar());
        }, cancellationToken: TestCancellation);
    }

    private (UniqueKillSwitch Kill, ISinkQueue<EntityUpdate<CounterState, CounterEvent>> Queue) Subscribe(
        IDurableEntityRef<CounterCommand, CounterState, CounterEvent> entity, long? after = null) => entity.Subscribe(after is { } sequence ? Offset.Sequence(sequence) : Offset.NoOffset())
        .ViaMaterialized(KillSwitches.Single<EntityUpdate<CounterState, CounterEvent>>(), Keep.Right)
        .ToMaterialized(Sink.Queue<EntityUpdate<CounterState, CounterEvent>>(), Keep.Both)
        .Run(Sys.Materializer());

    private static async Task<EntityUpdate<CounterState, CounterEvent>> Next(
        ISinkQueue<EntityUpdate<CounterState, CounterEvent>> queue) =>
        (await queue.PullAsync().WaitAsync(TimeSpan.FromSeconds(15), TestCancellation)).Value;

    [Fact]
    public async Task Stored_snapshot_and_journal_tail_can_be_queried_without_reactivating_a_passivated_publisher()
    {
        await Populate();
        var counter = CounterRef();
        var actor = await Sys.ActorSelection("/user/durable-counter/a").ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);
        Watch(actor);
        await counter.Ask(new Shutdown(), AskCancellation());
        await ExpectTerminatedAsync(actor, cancellationToken: TestCancellation);
        var subscription = Subscribe(counter);
        try
        {
            var baseline = Assert.IsType<StateSnapshot<CounterState>>((await Next(subscription.Queue)).Value);
            Assert.Equal(2, baseline.SequenceNr);
            Assert.Equal(new CounterState(3, false), baseline.State);
            Assert.Equal(new EntityIdentity(Counter.TypeName, new EntityId("a")), baseline.Publisher);
            var tail = Assert.IsType<CommittedEvent<CounterEvent>>((await Next(subscription.Queue)).Value);
            Assert.Equal(3, tail.SequenceNr);
            Assert.Equal(new Incremented(4), tail.Event.Value);
            Assert.Equal(1, Volatile.Read(ref _initializations));

            await counter.Ask(new Increment(8), AskCancellation());
            var live = Assert.IsType<CommittedEvent<CounterEvent>>((await Next(subscription.Queue)).Value);
            Assert.Equal(4, live.SequenceNr);
            Assert.Equal(new Incremented(8), live.Event.Value);
            Assert.Equal(2, Volatile.Read(ref _initializations));
        }
        finally { subscription.Kill.Shutdown(); }
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData(1L, true)]
    [InlineData(2L, false)]
    public async Task Real_queries_honor_snapshot_fallback_and_exclusive_resume_positions(long? after, bool baselineExpected)
    {
        await Populate();
        var subscription = Subscribe(CounterRef(), after);
        try
        {
            if (baselineExpected)
                Assert.Equal(2, Assert.IsType<StateSnapshot<CounterState>>((await Next(subscription.Queue)).Value).SequenceNr);
            Assert.Equal(3, Assert.IsType<CommittedEvent<CounterEvent>>((await Next(subscription.Queue)).Value).SequenceNr);
        }
        finally { subscription.Kill.Shutdown(); }
    }

    [Fact]
    public async Task Querying_an_unwritten_entity_returns_initial_state_without_creating_its_actor()
    {
        var subscription = Subscribe(CounterRef("unwritten"));
        try
        {
            var baseline = Assert.IsType<StateSnapshot<CounterState>>((await Next(subscription.Queue)).Value);
            Assert.Equal(new CounterState(0, false), baseline.State);
            Assert.Equal(0, baseline.SequenceNr);
            Assert.Equal(1, Volatile.Read(ref _initializations)); // InitialState is evaluated for the query's zero baseline.
            Sys.ActorSelection("/user/durable-counter/*").Tell(new Identify("publisher"), TestActor);
            var identity = await ExpectMsgAsync<ActorIdentity>(cancellationToken: TestCancellation);
            Assert.Null(identity.Subject);
        }
        finally { subscription.Kill.Shutdown(); }
    }
}
