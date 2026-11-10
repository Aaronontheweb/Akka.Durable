using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures.WorkflowProbe;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Reminders;
using LinqToDB;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Probe for the workflow layer. A workflow would be hosted as an entity whose state, command and event
/// types belong to the library and carry the author's types inside them. These specs write such wrappers
/// to a real journal (Akka.Persistence.Sql on SQLite), stop the entity, and let a new incarnation recover
/// from the stored bytes.
/// </summary>
public abstract class WorkflowWrapperJournalSpecs(ITestOutputHelper output)
    : Akka.Hosting.TestKit.TestKit(output: output)
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-wf-{Guid.NewGuid():N}.db");

    private static readonly CheckoutInput Input = new("order-7", 4200);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected abstract EntityDefinition<WorkflowState, WorkflowCommand, WorkflowEvent> Definition { get; }

    protected abstract long ExpectedSnapshots { get; }

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        base.ConfigureServices(context, services);
        services.AddDurableEntities();
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder
            .WithSqlPersistence(
                connectionString: $"Data Source={_databasePath}",
                providerName: ProviderName.SQLiteMS)
            // The author registers only the serializer for their own types.
            .WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails)
            // The library would register this one itself.
            .WithCustomSerializer(WorkflowWrapperSerializer.CreateSerializerDetails)
            .WithDurableEntity(Definition);

    protected override async Task AfterAllAsync()
    {
        await base.AfterAllAsync();
        SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }

    private static CancellationToken AskTimeout()
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        return cts.Token;
    }

    private long CountRows(string table)
    {
        using var connection = new SqliteConnection($"Data Source={_databasePath}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (long)command.ExecuteScalar()!;
    }

    /// <summary>Stops the running entity actor, so the next message starts one that has to recover.</summary>
    private async Task StopEntity(string entityId)
    {
        var entity = await Sys
            .ActorSelection($"/user/{LocalEntityHost.ActorName(Checkout.TypeName)}/{entityId}")
            .ResolveOne(TimeSpan.FromSeconds(5), Ct);
        Watch(entity);
        entity.Tell(EntityActor<WorkflowState, WorkflowCommand, WorkflowEvent>.StopEntity.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
    }

    [Fact]
    public async Task Wrappers_carry_the_authors_unions_through_a_real_journal()
    {
        var checkout = Host.Services
            .GetRequiredService<IDurableEntityRefs<WorkflowCommand, WorkflowState, WorkflowEvent>>()
            .Get(new EntityId("order-7"));

        checkout.Tell(new StartWorkflow(Input));
        checkout.Tell(new Deliver((CheckoutFact)new Reserved("res-1")));
        // Moves to a state that holds the state it came from: a union inside its own case.
        checkout.Tell(new Deliver((CheckoutFact)new WorkStuck("card service")));
        // The read is queued behind the three commands.
        Assert.Equal(3, (await checkout.GetStateAsync(AskTimeout())).SequenceNr);

        Assert.Equal(3, CountRows("journal"));
        // A snapshot is saved in the background: wait for it, so that recovery has to read it.
        await AwaitAssertAsync(() => Assert.Equal(ExpectedSnapshots, CountRows("snapshot")), cancellationToken: Ct);

        await StopEntity("order-7");

        // The new entity actor has nothing in memory: recovery reads and deserializes the stored bytes.
        var snapshot = await checkout.GetStateAsync(AskTimeout());

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
        checkout.Tell(new Deliver((CheckoutFact)new OperatorSaidRetry()));
        // A fact that holds another union.
        checkout.Tell(new Deliver((CheckoutFact)new PaymentAnswered(new Approved("charge-9"))));
        var last = await checkout.GetStateAsync(AskTimeout());

        Assert.Equal(new Finished(Input, (CheckoutOutcome)new OrderPlaced("charge-9")), last.State.Value);
        Assert.Equal(5, last.SequenceNr);
    }
}

/// <summary>Recovery from events alone.</summary>
public sealed class WorkflowWrapperEventJournalSpecs(ITestOutputHelper output) : WorkflowWrapperJournalSpecs(output)
{
    protected override EntityDefinition<WorkflowState, WorkflowCommand, WorkflowEvent> Definition => Checkout.Definition;

    protected override long ExpectedSnapshots => 0;
}

/// <summary>Recovery from a snapshot of the wrapper state, plus the event that followed it.</summary>
public sealed class WorkflowWrapperSnapshotJournalSpecs(ITestOutputHelper output) : WorkflowWrapperJournalSpecs(output)
{
    protected override EntityDefinition<WorkflowState, WorkflowCommand, WorkflowEvent> Definition =>
        Checkout.Definition with { Options = EntityOptions.Default with { SnapshotEvery = 2 } };

    protected override long ExpectedSnapshots => 1;
}

/// <summary>
/// The other serialization boundaries a workflow's wrappers cross: the state read, reminders and sharding.
/// The serializers are the ones the test host registers, the way an application would.
/// </summary>
public sealed class WorkflowWrapperSerializationSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    private static readonly CheckoutInput Input = new("order-7", 4200);

    private static readonly WorkflowState DeepState = new Running(
        Input,
        (CheckoutState)new NeedsAttention(new NeedsAttention(new Charging("res-1"), "first"), "second"),
        [0, 1, 2],
        (CheckoutFact)new PaymentAnswered(new Refused("no funds")));

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder
            .WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails)
            .WithCustomSerializer(WorkflowWrapperSerializer.CreateSerializerDetails)
            .WithDurableEntity(Checkout.Definition);

    private object RoundTrip(object message)
    {
        var serialization = Sys.Serialization;
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
