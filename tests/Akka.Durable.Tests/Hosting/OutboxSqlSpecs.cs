using Akka.Reminders;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Durable.Tests.Fixtures;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>A retried effect across real restarts: new actor systems read the journal from SQLite.</summary>
public sealed class OutboxSqlSpecs : IDisposable
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-outbox-{Guid.NewGuid():N}.db");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static CancellationToken AskTimeout()
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        return cts.Token;
    }

    // Attempts that fail leave 1.5 s before the next; an attempt that hangs is cut off after 1 s. So a host that stops
    // soon after attempt 2 starts has journaled nothing more.
    private static readonly EntityOptions Options = EntityOptions.Default with { EffectTimeout = TimeSpan.FromSeconds(1) };

    private async Task<IHost> StartHost(ProbeWorld world)
    {
        var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>().AddCounterHandlers();
                services.AddSingleton(world)
                    .AddEffectHandler<Probe, CounterCommand, ProbeHandler>(
                        Retry.UntilAcknowledged(TimeSpan.FromMilliseconds(1500), TimeSpan.FromMilliseconds(1500)))
                    .AddEffectHandler<Note, NoteHandler>();
                services.AddAkka("outbox-sql", builder => builder
                    .WithSqlPersistence(connectionString: $"Data Source={_databasePath}", providerName: ProviderName.SQLiteMS)
                    .WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)))
                    .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
                    .WithCustomSerializer(ProbeSerializer.CreateSerializerDetails)
                    .WithDurableEntity(ProbeEntity.Definition(world, Options)));
            })
            .Build();

        await host.StartAsync(Ct);
        return host;
    }

    private static IDurableEntityRef<CounterCommand, CounterState, CounterEvent> CounterRef(IHost host) =>
        host.Services.GetRequiredService<IDurableEntityRefs<CounterCommand, CounterState, CounterEvent>>().Get(new EntityId("a"));

    private static async Task<CounterState> StateOf(IHost host) => (await CounterRef(host).GetStateAsync(AskTimeout())).State;

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
    public async Task A_retried_effect_keeps_its_id_and_its_attempt_count_across_a_restart_and_is_answered_once()
    {
        // Attempt 1 fails, attempt 2 hangs, and the host stops with attempt 2 in flight.
        var first = new ProbeWorld();
        first.OnProbe = (_, ct) => first.ProbeCalls.Count == 1
            ? throw new InvalidOperationException("not yet")
            : Task.Delay(Timeout.Infinite, ct).ContinueWith<CounterCommand>(_ => default);
        using (var one = await StartHost(first))
        {
            CounterRef(one).Tell(new NoHandler("issue:a"));
            await Until(() => first.ProbeCalls.Count == 2);
            await one.StopAsync(Ct);
        }

        // The second system reads the policy, the attempt count and the id back from the journal and attempts again.
        var second = new ProbeWorld();
        using (var two = await StartHost(second))
        {
            await StateOf(two); // starts the entity: nothing else wakes it, reminders are not stored in SQLite
            await Until(() => second.ProbeCalls.Count == 1);
            await Until(() => StateOf(two).Result.Count == 101);
            Assert.Equal(first.ProbeCalls.Last().Delivery.Id, second.ProbeCalls.Single().Delivery.Id);
            Assert.Equal(first.ProbeCalls.First().Delivery.Id, first.ProbeCalls.Last().Delivery.Id);
            Assert.Equal(3, second.ProbeCalls.Single().Delivery.Attempt);
            Assert.Equal(["answer:a"], second.Decided.Where(d => d.StartsWith("answer:", StringComparison.Ordinal)));
            await two.StopAsync(Ct);
        }

        // The third system finds the effect settled: nothing runs and nothing is decided.
        var third = new ProbeWorld();
        using var last = await StartHost(third);
        Assert.Equal(new CounterState(101, false), await StateOf(last));
        await Task.Delay(TimeSpan.FromSeconds(3), Ct);
        Assert.Empty(third.ProbeCalls);
        Assert.Empty(third.Decided);
        await last.StopAsync(Ct);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }
}
