using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Reminders;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>Retried workflow work across real restarts: new actor systems read the journal from SQLite.</summary>
public sealed class WorkflowRetrySqlSpecs : IDisposable
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-wf-retry-{Guid.NewGuid():N}.db");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static CancellationToken AskTimeout()
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        return cts.Token;
    }

    // An attempt that hangs is cut off after 1 s; the wait between attempts is 1.5 s.
    private static readonly TimeSpan EffectTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Wait = TimeSpan.FromMilliseconds(1500);

    private async Task<IHost> StartHost(CheckoutServices services)
    {
        var definition = Checkout.Definition with { Options = EntityOptions.Default with { EffectTimeout = EffectTimeout } };
        var host = new HostBuilder()
            .ConfigureServices(collection =>
            {
                collection.AddDurableEntities();
                collection.AddSingleton(services)
                    .AddEffectHandler<ReserveStock, CheckoutFact, ReserveStockHandler>()
                    .AddEffectHandler<ChargeCard, CheckoutFact, ChargeCardHandler>(Retry.Every(Wait, attempts: 4))
                    .AddEffectHandler<UndoOrder, CheckoutFact, UndoOrderHandler>()
                    .AddEffectHandler<OpenSupportTicket, CheckoutFact, OpenSupportTicketHandler>();
                collection.AddAkka("workflow-retry-sql", builder => builder
                    .WithSqlPersistence(connectionString: $"Data Source={_databasePath}", providerName: ProviderName.SQLiteMS)
                    .WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)))
                    .WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails)
                    .WithDurableWorkflow(definition));
            })
            .Build();

        await host.StartAsync(Ct);
        return host;
    }

    private static IWorkflowRef<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> CheckoutRef(IHost host) =>
        host.Services.GetRequiredService<IWorkflowRefs<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>()
            .Get(new WorkflowId("sql-1"));

    /// <summary>The deliveries of the attempts at the charge, in order.</summary>
    private static EffectDelivery[] Charges(CheckoutServices services) =>
        [.. services.Issued.Zip(services.Deliveries).Where(pair => pair.Item1 is ChargeCard).Select(pair => pair.Item2)];

    private static async Task Until(Func<bool> done)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!done())
        {
            Assert.True(DateTime.UtcNow < deadline, "Timed out waiting.");
            await Task.Delay(20, Ct);
        }
    }

    [Fact]
    public async Task The_attempt_count_and_the_next_attempt_date_survive_a_restart_and_the_work_is_answered_once()
    {
        // Attempt 1 fails, attempt 2 hangs, and the host stops with attempt 2 in flight.
        var started = new ConcurrentQueue<DateTimeOffset>();
        var first = new CheckoutServices();
        first.Charge = (_, _) =>
        {
            started.Enqueue(DateTimeOffset.UtcNow);
            return started.Count == 1
                ? Task.FromException<CheckoutFact>(new InvalidOperationException("not yet"))
                : Task.Delay(Timeout.Infinite).ContinueWith<CheckoutFact>(_ => default);
        };
        using (var one = await StartHost(first))
        {
            await CheckoutRef(one).Start(new PlaceOrder("sql-1", 100), AskTimeout());
            await Until(() => started.Count == 2);
            await one.StopAsync(Ct);
        }

        var attemptTwoAt = started.ElementAt(1);
        var attemptTwo = Charges(first)[1];

        // The new system reads the attempt count and the policy from the journal. Attempt 2 counts as failed, and
        // attempt 3 is due after its time limit and the wait: not at once.
        var second = new CheckoutServices();
        var thirdStarted = new ConcurrentQueue<DateTimeOffset>();
        second.Charge = (effect, _) =>
        {
            thirdStarted.Enqueue(DateTimeOffset.UtcNow);
            return Task.FromResult<CheckoutFact>(new Charged("charge-sql"));
        };
        using (var two = await StartHost(second))
        {
            var checkout = CheckoutRef(two);
            await checkout.GetStateAsync(AskTimeout());
            Assert.True(DateTimeOffset.UtcNow < attemptTwoAt + EffectTimeout + Wait, "The restart was too slow: the test proves nothing.");
            Assert.Empty(thirdStarted);

            Assert.Equal(new OrderPlaced("charge-sql"), (await checkout.WaitForOutcome(TimeSpan.FromMilliseconds(20), AskTimeout())).Value);

            var attemptThree = Assert.Single(Charges(second));
            Assert.Equal(3, attemptThree.Attempt);
            Assert.Equal(attemptTwo.Id, attemptThree.Id);
            Assert.True(thirdStarted.Single() >= attemptTwoAt + EffectTimeout + Wait - TimeSpan.FromMilliseconds(250),
                $"Attempt 3 started {thirdStarted.Single() - attemptTwoAt} after attempt 2.");
            await two.StopAsync(Ct);
        }

        // A third system finds the work answered: the workflow is finished and nothing is attempted.
        var third = new CheckoutServices();
        using var last = await StartHost(third);
        Assert.Equal(new OrderPlaced("charge-sql"), (await CheckoutRef(last).WaitForOutcome(TimeSpan.FromMilliseconds(20), AskTimeout())).Value);
        Assert.Empty(third.Issued);
        await last.StopAsync(Ct);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }
}
