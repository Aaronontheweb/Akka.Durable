using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures.WorkflowProbe;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Reminders;
using Akka.Reminders.Serialization;
using Akka.Serialization;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Probe for the workflow layer. A workflow would be hosted as an entity whose state, command and event
/// types belong to the library and carry the author's types inside them. These specs write such wrappers
/// to a real journal (Akka.Persistence.Sql on SQLite) and recover them in a new ActorSystem.
/// </summary>
public sealed class WorkflowWrapperJournalSpecs : IDisposable
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-wf-{Guid.NewGuid():N}.db");

    private static readonly CheckoutInput Input = new("order-7", 4200);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static CancellationToken AskTimeout()
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        return cts.Token;
    }

    private async Task<IHost> StartHost(EntityDefinition<WorkflowState, WorkflowCommand, WorkflowEvent> definition)
    {
        var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddAkka("workflow-wrapper-round-trip", builder => builder
                    .WithSqlPersistence(
                        connectionString: $"Data Source={_databasePath}",
                        providerName: ProviderName.SQLiteMS)
                    // The author registers only the serializer for their own types.
                    .WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails)
                    // The library would register this one itself.
                    .WithCustomSerializer(WorkflowWrapperSerializer.CreateSerializerDetails)
                    .WithDurableEntity(definition));
            })
            .Build();

        await host.StartAsync(Ct);
        return host;
    }

    private static IDurableEntityRef<WorkflowCommand, WorkflowState, WorkflowEvent> CheckoutRef(IHost host) =>
        host.Services.GetRequiredService<IDurableEntityRefs<WorkflowCommand, WorkflowState, WorkflowEvent>>()
            .Get(new EntityId("order-7"));

    private long CountRows(string table)
    {
        using var connection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={_databasePath}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (long)command.ExecuteScalar()!;
    }

    private async Task WriteThenRecover(
        EntityDefinition<WorkflowState, WorkflowCommand, WorkflowEvent> definition, long expectedSnapshots)
    {
        using (var writer = await StartHost(definition))
        {
            var checkout = CheckoutRef(writer);
            checkout.Tell(new StartWorkflow(Input));
            checkout.Tell(new Deliver((CheckoutFact)new Reserved("res-1")));
            // Moves to a state that holds the state it came from: a union inside its own case.
            checkout.Tell(new Deliver((CheckoutFact)new WorkStuck("card service")));
            // The read is queued behind the three commands.
            Assert.Equal(3, (await checkout.GetStateAsync(AskTimeout())).SequenceNr);
            await writer.StopAsync(Ct);
        }

        Assert.Equal(3, CountRows("journal"));
        Assert.Equal(expectedSnapshots, CountRows("snapshot"));

        // A new ActorSystem has nothing in memory: recovery reads and deserializes the stored bytes.
        using var reader = await StartHost(definition);
        var recovered = CheckoutRef(reader);
        var snapshot = await recovered.GetStateAsync(AskTimeout());

        var running = Assert.IsType<Running>(snapshot.State.Value);
        // The author's values come back as the author's union types, not as the case types.
        Assert.IsType<CheckoutState>(running.State);
        Assert.IsType<CheckoutFact>(running.LastFact);
        Assert.Equal(
            new Running(
                Input,
                (CheckoutState)new NeedsAttention(new Charging("res-1"), "card service"),
                [0, 1],
                (CheckoutFact)new WorkStuck("card service")),
            running);

        // The recovered values go back into the author's typed Decide, and the workflow runs to its end.
        recovered.Tell(new Deliver((CheckoutFact)new OperatorSaidRetry()));
        // A fact that holds another union.
        recovered.Tell(new Deliver((CheckoutFact)new PaymentAnswered(new Approved("charge-9"))));
        var last = await recovered.GetStateAsync(AskTimeout());

        Assert.Equal(new Finished(Input, (CheckoutOutcome)new OrderPlaced("charge-9")), last.State.Value);
        Assert.Equal(5, last.SequenceNr);
        await reader.StopAsync(Ct);
    }

    [Fact]
    public Task Wrapper_events_carry_the_authors_unions_through_a_real_journal() =>
        WriteThenRecover(Checkout.Definition, expectedSnapshots: 0);

    [Fact]
    public Task Wrapper_snapshots_carry_the_authors_unions_through_a_real_journal() =>
        WriteThenRecover(
            Checkout.Definition with { Options = EntityOptions.Default with { SnapshotEvery = 2 } },
            expectedSnapshots: 1);

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }
}

/// <summary>
/// The other serialization boundaries a workflow's wrappers cross: the state read, reminders and sharding.
/// </summary>
public sealed class WorkflowWrapperSerializationSpecs : IAsyncLifetime
{
    private readonly ActorSystem _system = ActorSystem.Create("workflow-wrapper-serialization",
        BootstrapSetup.Create().And(SerializationSetup.Create(system =>
        [
            CheckoutSerializer.CreateSerializerDetails(system),
            WorkflowWrapperSerializer.CreateSerializerDetails(system),
            DurableEntitySerializer.CreateSerializerDetails(system),
            SerializerDetails.Create("reminder-serializer", new ReminderSerializer(system),
                [typeof(IReminderWireMessage)]),
        ])));

    private static readonly CheckoutInput Input = new("order-7", 4200);

    private static readonly WorkflowState DeepState = new Running(
        Input,
        (CheckoutState)new NeedsAttention(new NeedsAttention(new Charging("res-1"), "first"), "second"),
        [0, 1, 2],
        (CheckoutFact)new PaymentAnswered(new Refused("no funds")));

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _system.Terminate();

    private object RoundTrip(object message)
    {
        var serialization = _system.Serialization;
        var serializer = serialization.FindSerializerFor(message);
        var bytes = serializer.ToBinary(message);
        var manifest = Akka.Serialization.Serialization.ManifestFor(serializer, message);
        return serialization.Deserialize(bytes, serializer.Identifier, manifest);
    }

    [Fact]
    public void Wrapper_state_round_trips_with_nested_unions_a_list_and_an_empty_optional_value()
    {
        Assert.Equal(DeepState, RoundTrip(DeepState));

        WorkflowState fresh = new Running(Input, (CheckoutState)new Reserving(), [], null);
        Assert.Equal(fresh, RoundTrip(fresh));
    }

    [Fact]
    public void State_read_reply_carries_the_wrapper_state()
    {
        // Framework reply -> wrapper state -> author's state: three serializers, one inside the next.
        var reply = (EntityStateReply)RoundTrip(new EntityStateReply(DeepState, 12));

        Assert.Equal(DeepState, reply.State);
    }

    [Fact]
    public void Reminder_envelope_carries_a_wrapper_command()
    {
        // How a workflow timer's fact would travel from reminder storage back to the workflow.
        WorkflowCommand command = new Deliver((CheckoutFact)new WorkStuck("confirmation overdue"));
        var sent = new ReminderEnvelope(new ReminderEntity("checkout-probe", "order-7"), new ReminderKey("timer"),
            new DateTimeOffset(2026, 11, 9, 12, 0, 0, TimeSpan.Zero), ReminderDeadline.Infinite, command);

        var received = (ReminderEnvelope)RoundTrip(sent);

        Assert.Equal(command, received.Message);
    }

    [Fact]
    public void Sharding_envelope_carries_a_wrapper_command()
    {
        WorkflowCommand command = new StartWorkflow(Input);

        var received = (ShardingEnvelope)RoundTrip(new ShardingEnvelope("order-7", command));

        Assert.Equal(command, received.Message);
    }
}
