using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Reminders;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>One smoke spec for each behaviour of a recorded <c>Execute</c> in a hosted entity.</summary>
public sealed class RecordedExecuteSmokeSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    [Fact]
    public async Task A_handler_with_no_answer_is_acknowledged_and_gets_the_effects_id()
    {
        await CounterRef("a1").Ask(new Announce("hello"), AskCancellation());

        await AwaitAssertAsync(async () => Assert.Empty(await PendingEffects<CounterCommand>("a1")),
            cancellationToken: TestCancellation);
        var entry = Assert.Single(Recorder.Entries, e => e.StartsWith("announce:", StringComparison.Ordinal));
        Assert.StartsWith("announce:hello:counter/a1/", entry);
        Assert.EndsWith(":1", entry); // the first attempt
    }

    [Fact]
    public async Task A_handler_that_throws_gives_up_as_failed_and_Decide_gets_the_give_up()
    {
        await CounterRef("f1").Ask(new ExplodeLater(), AskCancellation());

        await AwaitAssertAsync(async () =>
            Assert.Equal("explode:Failed", (await DurableCounterRef("f1").GetStateAsync(AskCancellation())).State.LastGaveUp),
            cancellationToken: TestCancellation);
        Assert.Empty(await PendingEffects<CounterCommand>("f1"));
    }
}

/// <summary>The host refuses to start when the effects of an entity are not set up right.</summary>
public sealed class EffectRouteStartupSpecs
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>An effect of the counter with no serializer: it carries no [AkkaSerializable].</summary>
    public sealed record PlainEffect(int Value);

    public sealed class PlainEffectHandler : IEffectHandler<PlainEffect>
    {
        public Task HandleAsync(PlainEffect effect, EffectDelivery delivery, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    /// <summary>A handler for a counter effect that answers with another entity's command.</summary>
    public sealed class ComputeForRailway : IEffectHandler<Compute, RailwayCommand>
    {
        public Task<RailwayCommand> HandleAsync(Compute effect, EffectDelivery delivery, CancellationToken cancellationToken) =>
            Task.FromResult<RailwayCommand>(new Adjust(1));
    }

    private static async Task<string> StartupFailure(Action<IServiceCollection> handlers)
    {
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                handlers(services);
                services.AddAkka("effect-routes", builder => builder
                    .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
                    .WithDurableEntity(Counter.Definition));
            })
            .Build();

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync(Ct));
        await host.StopAsync(Ct);
        return ex.ToString();
    }

    [Fact]
    public async Task An_effect_without_a_serializer_fails_startup_and_the_error_names_it_and_its_handler()
    {
        var message = await StartupFailure(services => services.AddEffectHandler<PlainEffect, PlainEffectHandler>());

        Assert.Contains("Durable entity [counter] cannot start: the effect PlainEffect, handled by PlainEffectHandler", message);
        Assert.Contains("make it implement ICounterProtocol", message);
    }

    [Fact]
    public void A_second_handler_for_one_effect_type_is_refused()
    {
        var services = new ServiceCollection().AddEffectHandler<Compute, CounterCommand, ComputeHandler>();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddEffectHandler<Compute, RailwayCommand, ComputeForRailway>());

        Assert.Contains("Two effect handlers are registered for the effect Compute", ex.Message);
    }
}

/// <summary>A workflow's work gets an id made of the workflow's address, the entry and the position.</summary>
public sealed class WorkflowEffectDeliverySpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    [Fact]
    public async Task Each_piece_of_work_gets_the_id_of_its_entry_and_position()
    {
        var checkout = CheckoutRef("c-9");
        await checkout.Start(new PlaceOrder("o9", 100), AskCancellation());
        await checkout.WaitForOutcome(AskCancellation());

        Assert.Equal(
            [new EffectDelivery(new EffectId("checkouts/c-9/1.0", 1), 1), new EffectDelivery(new EffectId("checkouts/c-9/2.0", 2), 1)],
            Services.Deliveries);
    }
}

/// <summary>
/// The library's records written to a real journal and snapshot store (Akka.Persistence.Sql on SQLite) and read back
/// by a new ActorSystem.
/// </summary>
public sealed class RecordedExecuteSqlSpecs : IDisposable
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
                services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>().AddCounterHandlers();
                services.AddAkka("recorded-execute-sql", builder => builder
                    .WithSqlPersistence(connectionString: $"Data Source={_databasePath}", providerName: ProviderName.SQLiteMS)
                    .WithLocalReminders(reminders => reminders.WithResolver(system => new DurableEntityReminderResolver(system)))
                    .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
                    .WithDurableEntity(definition));
            })
            .Build();

        await host.StartAsync(Ct);
        return host;
    }

    private static IDurableEntityRef<CounterCommand, CounterState, CounterEvent> CounterRef(IHost host) =>
        host.Services.GetRequiredService<IDurableEntityRefs<CounterCommand, CounterState, CounterEvent>>()
            .Get(new EntityId("a"));

    // 100: recovery reads the effect from the journal. 1: from the snapshot taken after its write.
    [Theory]
    [InlineData(100)]
    [InlineData(1)]
    public async Task An_effect_pending_in_stored_bytes_comes_back_as_interrupted(int snapshotEvery)
    {
        var definition = Counter.Definition with { Options = EntityOptions.Default with { SnapshotEvery = snapshotEvery } };
        var request = new IncrementViaService(2, Wait: true);

        using (var writer = await StartHost(definition))
        {
            await CounterRef(writer).Ask(new Increment(1), AskTimeout());
            CounterRef(writer).Tell(request);

            // The handler runs only after the write, and it waits on a gate that never opens.
            var recorder = writer.Services.GetRequiredService<EffectRecorder>();
            while (!recorder.Entries.Contains("compute:2")) await Task.Delay(20, Ct);
            await writer.StopAsync(Ct);
        }

        using var reader = await StartHost(definition);
        var state = (await CounterRef(reader).GetStateAsync(AskTimeout())).State;

        Assert.Equal(new CounterState(1, false, Pending: null, LastGaveUp: "compute:Interrupted"), state);
        Assert.DoesNotContain("compute:2", reader.Services.GetRequiredService<EffectRecorder>().Entries);
        await reader.StopAsync(Ct);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }
}
