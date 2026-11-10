using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Reminders;
using LinqToDB;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A workflow's state and facts written to a real journal (Akka.Persistence.Sql on SQLite) and read back by
/// a new actor. The library's own types carry the author's types inside them, and every value is serialized
/// to bytes by the serializer bound to its type.
/// </summary>
public abstract class WorkflowJournalSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-wf-{Guid.NewGuid():N}.db");

    protected abstract int SnapshotEvery { get; }

    protected abstract long ExpectedSnapshots { get; }

    // The test host's default journal keeps objects in memory. The workflow is pointed at the SQL plugins.
    protected override WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        Checkout.Definition with
        {
            Options = new WorkflowOptions
            {
                Hosting = EntityOptions.Default with
                {
                    JournalPluginId = "akka.persistence.journal.sql",
                    SnapshotPluginId = "akka.persistence.snapshot-store.sql",
                    SnapshotEvery = SnapshotEvery,
                },
            },
        };

    protected override void ConfigureMore(AkkaConfigurationBuilder builder) =>
        builder.WithSqlPersistence(
            connectionString: $"Data Source={_databasePath}",
            providerName: ProviderName.SQLiteMS,
            isDefaultPlugin: false);

    protected override async Task AfterAllAsync()
    {
        await base.AfterAllAsync();
        SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }

    private long CountRows(string table)
    {
        using var connection = new SqliteConnection($"Data Source={_databasePath}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (long)command.ExecuteScalar()!;
    }

    [Fact]
    public async Task A_workflow_recovers_its_state_and_the_authors_values_from_stored_bytes()
    {
        // Declined, then an undo that gets no answer: the workflow waits in a state that holds an outcome.
        Services.Charge = (_, _) => Task.FromResult<CheckoutFact>(new Declined("no funds"));
        Services.Undo = (_, ct) => Never(ct);
        await CheckoutRef("o1").Start(new PlaceOrder("o1", 4200), AskCancellation());
        await AwaitRunning("o1", running => Assert.IsType<Undoing>(((CheckoutState)running.State).Value));

        await RestartWorkflow("o1");

        // The new actor has nothing in memory. It reads the stored bytes, and the recovered values go back
        // into the author's typed functions: the interrupted undo gives up, and the table moves on.
        var recovered = await SettledIn<NeedsAttention>("o1");
        Assert.IsType<CheckoutState>(recovered.State);
        Assert.Equal(
            new NeedsAttention(new Undoing(new OrderFailed("no funds")), "UndoOrder Interrupted"),
            ((CheckoutState)recovered.State).Value);

        // The start and four facts: reserved, declined, the give-up, and the ticket.
        var before = await HostedState("o1");
        Assert.Equal(5, before.SequenceNr);
        Assert.Equal(5, CountRows("journal"));
        // A snapshot is saved in the background: wait for it, so that the next recovery has to read it.
        await AwaitAssertAsync(() => Assert.Equal(ExpectedSnapshots, CountRows("snapshot")), cancellationToken: TestCancellation);

        await RestartWorkflow("o1");

        // A state that holds the state it came from, which holds an outcome: equal to the last value, to the tick.
        Assert.Equal(before, await HostedState("o1"));
        var status = Assert.IsType<WorkflowRunning>((await CheckoutRef("o1").Status(AskCancellation())).Value);
        Assert.Equal(nameof(NeedsAttention), status.State);
    }
}

/// <summary>Recovery from events alone.</summary>
public sealed class WorkflowEventJournalSpecs(ITestOutputHelper output) : WorkflowJournalSpecs(output)
{
    protected override int SnapshotEvery => 0;
    protected override long ExpectedSnapshots => 0;
}

/// <summary>Recovery from a snapshot of the workflow's state, plus the events that followed it.</summary>
public sealed class WorkflowSnapshotJournalSpecs(ITestOutputHelper output) : WorkflowJournalSpecs(output)
{
    protected override int SnapshotEvery => 2;
    protected override long ExpectedSnapshots => 2;
}

/// <summary>
/// The other serialization boundaries a workflow crosses between nodes: the start, its reply, and the status read.
/// The serializers are the ones the test host registers, the way an application would.
/// </summary>
public sealed class WorkflowWireSerializationSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private static readonly PlaceOrder Order = new("o1", 4200);

    private static readonly WfState DeepState = new WfRunning(
        Order,
        (CheckoutState)new NeedsAttention(new NeedsAttention(new Undoing(new OrderFailed("no funds")), "first"), "second"),
        Entry: 4,
        EnteredAt: new DateTimeOffset(2026, 11, 9, 12, 0, 0, TimeSpan.FromHours(-6)),
        Settled: [0, 2],
        Held: []);

    private static readonly DateTimeOffset At = new(2026, 11, 9, 12, 0, 0, TimeSpan.Zero);

    private object RoundTrip(object message)
    {
        var serialization = Sys.Serialization;
        var serializer = serialization.FindSerializerFor(message);
        var bytes = serializer.ToBinary(message);
        var manifest = Akka.Serialization.Serialization.ManifestFor(serializer, message);
        return serialization.Deserialize(bytes, serializer.Identifier, manifest);
    }

    [Fact]
    public void State_round_trips_with_the_authors_nested_unions()
    {
        Assert.Equal(DeepState, RoundTrip(DeepState));

        WfState finished = new WfFinished(Order, (CheckoutOutcome)new OrderPlaced("charge-9"), OutcomeKind.Succeeded,
            new DateTimeOffset(2026, 11, 9, 12, 0, 0, TimeSpan.Zero));
        Assert.Equal(finished, RoundTrip(finished));
    }

    [Fact]
    public void Status_read_reply_carries_the_state()
    {
        // Framework reply -> workflow state -> author's state: three serializers, one inside the next.
        var reply = (EntityStateReply)RoundTrip(new EntityStateReply(DeepState, 12));

        Assert.Equal(DeepState, reply.State);
    }

    [Fact]
    public void Sharding_envelope_carries_a_start()
    {
        WfCommand start = new WfStart(Order);

        var received = (ShardingEnvelope)RoundTrip(new ShardingEnvelope("o1", start));

        Assert.Equal(start, received.Message);
    }

    [Fact]
    public void Start_reply_carries_the_outcome_of_a_finished_workflow()
    {
        WfStartReply reply = new WfStartRefusedFinished((CheckoutOutcome)new OrderFailed("no funds"));

        Assert.Equal(reply, RoundTrip(reply));
    }

    [Fact]
    public void A_recorded_fact_round_trips_with_what_it_led_to()
    {
        WfEvent recorded = new WfFactRecorded(
            (CheckoutFact)new WorkStuck("ChargeCard", GaveUpReason.TimedOut),
            Answered: 0,
            At: new DateTimeOffset(2026, 11, 9, 12, 0, 0, TimeSpan.Zero),
            Step: new WfMoved((CheckoutState)new NeedsAttention(new Charging("res-1"), "ChargeCard TimedOut")),
            FromHeld: false);

        Assert.Equal(recorded, RoundTrip(recorded));
    }

    [Fact]
    public void State_round_trips_with_held_facts_in_order()
    {
        WfState state = new WfRunning(
            Order,
            (CheckoutState)new Charging("res-1"),
            Entry: 2,
            EnteredAt: At,
            Settled: [],
            Held:
            [
                (CheckoutFact)new CancelRequested("first"),
                (CheckoutFact)new OperatorSaidRetry(),
            ]);

        var received = Assert.IsType<WfRunning>(((WfState)RoundTrip(state)).Value);

        Assert.Equal(state.Value, received);
        Assert.Equal<object>((CheckoutFact)new CancelRequested("first"), received.Held[0]);
    }

    [Fact]
    public void A_held_fact_and_a_fact_taken_from_the_held_list_round_trip()
    {
        WfEvent held = new WfHeld((CheckoutFact)new CancelRequested("customer asked"));
        WfEvent recorded = new WfFactRecorded(
            (CheckoutFact)new CancelRequested("customer asked"),
            Answered: null,
            At: At,
            Step: new WfMoved((CheckoutState)new Undoing(new OrderCancelled("customer asked"))),
            FromHeld: true);

        Assert.Equal(held, RoundTrip(held));
        Assert.Equal(recorded, RoundTrip(recorded));
    }

    [Fact]
    public void Sharding_envelope_carries_a_fact_and_a_cancel()
    {
        WfCommand deliver = new WfDeliver((CheckoutFact)new OperatorSaidRetry(), WantsReply: true);
        WfCommand cancel = new WfCancel("customer asked", By: "support");

        Assert.Equal(deliver, ((ShardingEnvelope)RoundTrip(new ShardingEnvelope("o1", deliver))).Message);
        Assert.Equal(cancel, ((ShardingEnvelope)RoundTrip(new ShardingEnvelope("o1", cancel))).Message);
    }

    [Fact]
    public void State_round_trips_with_the_time_its_work_was_issued()
    {
        WfState state = new WfRunning(
            Order,
            (CheckoutState)new Charging("res-1"),
            Entry: 3,
            EnteredAt: At,
            Settled: [0],
            Held: [(CheckoutFact)new CancelRequested("late")],
            IssuedAt: At.AddMinutes(5));

        var received = Assert.IsType<WfRunning>(((WfState)RoundTrip(state)).Value);

        Assert.Equal(state.Value, received);
        Assert.Equal(At.AddMinutes(5), received.IssuedAt);
    }

    [Fact]
    public void Sharding_envelope_and_reminder_envelope_carry_a_wake()
    {
        WfCommand wake = new WfWake(At);
        WfCommand wakeWithoutTime = new WfWake();

        Assert.Equal(wake, ((ShardingEnvelope)RoundTrip(new ShardingEnvelope("o1", wake))).Message);
        Assert.Equal(wakeWithoutTime, ((ShardingEnvelope)RoundTrip(new ShardingEnvelope("o1", wakeWithoutTime))).Message);

        // How the workflow's reminder travels from reminder storage to the workflow.
        var sent = new ReminderEnvelope(new ReminderEntity("checkouts", "o1"), new ReminderKey(WorkflowWake.Key), At,
            ReminderDeadline.Infinite, wake);
        var received = (ReminderEnvelope)RoundTrip(sent);

        Assert.Equal(wake, received.Message);
        Assert.Equal(new ReminderKey(WorkflowWake.Key), received.Key);
        Assert.Equal(At, received.DueTimeUtc);
    }

    public static TheoryData<int> DeliveryReplies => [0, 1, 2, 3];

    [Theory]
    [MemberData(nameof(DeliveryReplies))]
    public void Each_delivery_reply_round_trips(int which)
    {
        WfDeliveryReply reply = which switch
        {
            0 => new WfDelivered(),
            1 => new WfHeldReply(),
            2 => new WfNotStartedReply(),
            _ => new WfClosedReply((CheckoutOutcome)new OrderCancelled("customer asked")),
        };

        Assert.Equal(reply, RoundTrip(reply));
    }
}
