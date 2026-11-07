using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Events and snapshots written to a real journal (Akka.Persistence.Sql on SQLite) and recovered by a new
/// ActorSystem: every value is serialized to bytes by its registered serializer and read back.
/// </summary>
public sealed class SqlJournalRoundTripSpecs : IDisposable
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-{Guid.NewGuid():N}.db");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static CancellationToken AskTimeout()
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        return cts.Token;
    }

    private async Task<IHost> StartHost(EntityDefinition<CounterState, CounterCommand, CounterEvent> definition)
    {
        var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddAkka("sql-round-trip", builder => builder
                    .WithSqlPersistence(
                        connectionString: $"Data Source={_databasePath}",
                        providerName: ProviderName.SQLiteMS)
                    .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
                    .WithDurableEntity(definition));
            })
            .Build();

        await host.StartAsync(Ct);
        return host;
    }

    private static IDurableEntityRef<CounterCommand, CounterState, CounterEvent> CounterRef(IHost host, string id) =>
        host.Services.GetRequiredService<IDurableEntityRefs<CounterCommand, CounterState, CounterEvent>>()
            .Get(new EntityId(id));

    private long CountRows(string table)
    {
        using var connection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={_databasePath}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (long)command.ExecuteScalar()!;
    }

    private async Task WriteThenRecover(
        EntityDefinition<CounterState, CounterCommand, CounterEvent> definition, long expectedSnapshots)
    {
        using (var writer = await StartHost(definition))
        {
            var counter = CounterRef(writer, "a");
            await counter.Ask<Accepted>(new Increment(1), AskTimeout());
            await counter.Ask<Accepted>(new Increment(2), AskTimeout());
            // With SnapshotEvery = 2 this third event lands after the snapshot, so recovery needs both.
            await counter.Ask<Accepted>(new LockCounter(), AskTimeout());
            await writer.StopAsync(Ct);
        }

        Assert.Equal(3, CountRows("journal"));
        Assert.Equal(expectedSnapshots, CountRows("snapshot"));

        // A new ActorSystem has nothing in memory: recovery reads and deserializes the stored bytes.
        using var reader = await StartHost(definition);
        var snapshot = await CounterRef(reader, "a").GetStateAsync(AskTimeout());

        Assert.Equal(new CounterState(3, true), snapshot.State);
        Assert.Equal(3, snapshot.SequenceNr);
        await reader.StopAsync(Ct);
    }

    [Fact]
    public Task Union_events_round_trip_through_a_real_journal() =>
        WriteThenRecover(Counter.Definition, expectedSnapshots: 0);

    [Fact]
    public Task Snapshots_and_union_events_round_trip_through_a_real_journal() =>
        WriteThenRecover(
            Counter.Definition with { Options = EntityOptions.Default with { SnapshotEvery = 2 } },
            expectedSnapshots: 1);

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }
}
