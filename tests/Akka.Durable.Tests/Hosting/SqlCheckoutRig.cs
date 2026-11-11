using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures.Checkouts;
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
/// The checkout workflow on a real journal and snapshot store (Akka.Persistence.Sql on SQLite). Each host is a new
/// actor system that reads what the last one wrote. Time is virtual: the wait before an attempt is a date, and a spec
/// sends the wake that the reminder would send.
/// </summary>
internal sealed class SqlCheckoutRig : IDisposable
{
    public const string WorkflowId = "w1";

    public static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    /// <summary>How long the wait that <see cref="AgainAfterRefusedUndo"/> asks for is.</summary>
    public static readonly TimeSpan UndoDelay = TimeSpan.FromMinutes(10);

    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-wf-rig-{Guid.NewGuid():N}.db");

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Null: no policy.</summary>
    public RetryPolicy? ChargePolicy { get; set; }

    public RetryPolicy? UndoPolicy { get; set; }

    public int SnapshotEvery { get; init; } = 100;

    /// <summary>How long one attempt at work may take before it is cut off.</summary>
    public TimeSpan EffectTimeout { get; init; } = TimeSpan.FromMilliseconds(300);

    /// <summary>A refused undo asks <c>Again(after: UndoDelay)</c> instead of <c>Again()</c>.</summary>
    public bool AgainAfterRefusedUndo { get; init; }

    /// <summary>The persistence id of the workflow's entity.</summary>
    public static string PersistenceId =>
        EntityOptions.DefaultPersistenceIdFactory(new EntityId(WorkflowId), new EntityTypeName("checkouts"));

    private WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition => Checkout.Definition with
    {
        Options = EntityOptions.Default with { EffectTimeout = EffectTimeout, SnapshotEvery = SnapshotEvery },
        Decide = (state, fact, ctx) => AgainAfterRefusedUndo && (state, fact) is (Undoing, UndoRefused)
            ? ctx.Again(UndoDelay)
            : Checkout.Decide(state, fact, ctx),
    };

    public async Task<IHost> StartAsync(CheckoutServices services)
    {
        var host = new HostBuilder()
            .ConfigureServices(collection =>
            {
                collection.AddDurableEntities();
                collection.AddSingleton(services)
                    .AddEffectHandler<ReserveStock, CheckoutFact, ReserveStockHandler>()
                    .AddEffectHandler<ChargeCard, CheckoutFact, ChargeCardHandler>(ChargePolicy)
                    .AddEffectHandler<UndoOrder, CheckoutFact, UndoOrderHandler>(UndoPolicy)
                    .AddEffectHandler<OpenSupportTicket, CheckoutFact, OpenSupportTicketHandler>();
                collection.AddAkka("workflow-sql-rig", builder => builder
                    .WithSqlPersistence(connectionString: $"Data Source={_databasePath}", providerName: ProviderName.SQLiteMS)
                    .WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)))
                    .WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails)
                    .WithDurableWorkflow(Definition));
            })
            .Build();

        await host.StartAsync(Ct);
        return host;
    }

    public static CancellationToken Within(TimeSpan time)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(time);
        return cts.Token;
    }

    public static IWorkflowRef<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Ref(IHost host) =>
        host.Services.GetRequiredService<IWorkflowRefs<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>()
            .Get(new WorkflowId(WorkflowId));

    private static IActorRef Region(IHost host) =>
        host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>>().ActorRef;

    /// <summary>The state of the entity that hosts the workflow, with its sequence number.</summary>
    public static async Task<StateSnapshot<WfState>> HostedState(IHost host) =>
        await new EntityRef<WfCommand, WfState, WfEvent>(new EntityId(WorkflowId), Region(host)).GetStateAsync(Within(TimeSpan.FromSeconds(30)));

    /// <summary>Waits until the hosted state is a <see cref="WfRunning"/> that <paramref name="accept"/> takes.</summary>
    public static async Task<StateSnapshot<WfState>> AwaitState(IHost host, Func<WfRunning, bool> accept)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            var state = await HostedState(host);
            if (state.State.Value is WfRunning running && accept(running)) return state;
            Assert.True(DateTime.UtcNow < deadline, $"Timed out waiting. The workflow is in {state.State.Value}.");
            await Task.Delay(20, Ct);
        }
    }

    /// <summary>Waits for a condition on the host's own state, such as rows in the database.</summary>
    public static async Task Until(Func<bool> done, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!done())
        {
            Assert.True(DateTime.UtcNow < deadline, $"Timed out waiting for {what}.");
            await Task.Delay(20, Ct);
        }
    }

    /// <summary>What the workflow's reminder does when it is delivered at <paramref name="at"/>.</summary>
    public static void Wake(IHost host, DateTimeOffset at) =>
        Region(host).Tell(new ShardingEnvelope(WorkflowId, (WfCommand)new WfWake(at)), ActorRefs.NoSender);

    /// <summary>The reminders stored for the workflow.</summary>
    public static async Task<IReadOnlyList<ScheduledReminder>> Reminders(IHost host)
    {
        var client = host.Services.GetRequiredService<ActorSystem>().ReminderClient().CreateClient("checkouts", WorkflowId);
        var listed = await client.ListRemindersAsync(Ct);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    /// <summary>Waits until the workflow has one reminder that <paramref name="accept"/> takes, and returns it.</summary>
    public static async Task<ScheduledReminder> AwaitReminder(IHost host, Func<ScheduledReminder, bool> accept)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            var reminders = await Reminders(host);
            if (reminders.Count == 1 && accept(reminders[0])) return reminders[0];
            Assert.True(DateTime.UtcNow < deadline, $"Timed out waiting for the reminder. Stored: {reminders.Count}.");
            await Task.Delay(20, Ct);
        }
    }

    /// <summary>The delivery of each attempt at the effect <typeparamref name="TEffect"/>, in order.</summary>
    public static EffectDelivery[] Deliveries<TEffect>(CheckoutServices services) =>
        [.. services.Issued.Zip(services.Deliveries).Where(pair => pair.Item1 is TEffect).Select(pair => pair.Item2)];

    // ---- The database ----

    public long Scalar(string sql, params (string Name, object Value)[] parameters)
    {
        using var connection = new SqliteConnection($"Data Source={_databasePath}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        return Convert.ToInt64(command.ExecuteScalar() ?? 0L, System.Globalization.CultureInfo.InvariantCulture);
    }

    public int Execute(string sql, params (string Name, object Value)[] parameters)
    {
        using var connection = new SqliteConnection($"Data Source={_databasePath}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value);
        return command.ExecuteNonQuery();
    }

    /// <summary>The rows in <paramref name="table"/>; 0 before the plugin has created the table, which it does on first use.</summary>
    public long Rows(string table) =>
        Scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = @name", ("@name", table)) == 0
            ? 0
            : Scalar($"SELECT COUNT(*) FROM {table}");

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }
}
