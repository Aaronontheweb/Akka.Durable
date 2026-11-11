using Akka.Reminders;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A recorded Execute through the public hosting API, in both hosting modes. The counter is scripted by
/// <see cref="ProbeEntity"/>: see its commands.
/// </summary>
public abstract class ProbeHostingSpec(ITestOutputHelper output) : CounterHostingSpec(output)
{
    protected static readonly TimeSpan FirstAskTimeout = TimeSpan.FromSeconds(30);

    protected ProbeWorld World { get; } = new();

    protected virtual EntityOptions Options => EntityOptions.Default with { EffectTimeout = TimeSpan.FromMilliseconds(500) };

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        ProbeEntity.Definition(World, Options);

    protected abstract string EntityPath(string id);

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        base.ConfigureTestServices(context, services);
        services.AddProbe(World);
    }

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder)
    {
        builder.WithCustomSerializer(ProbeSerializer.CreateSerializerDetails);
        if (Sharding is not null) ConfigureSingleNodeCluster(builder);
    }

    protected void Say(string id, string note) => CounterRef(id).Tell(new NoHandler(note), TestActor);

    protected async Task<CounterState> StateOf(string id, TimeSpan? timeout = null) =>
        (await DurableCounterRef(id).GetStateAsync(AskCancellation(timeout))).State;

    protected Task AwaitCount(string id, int count) =>
        AwaitAssertAsync(async () => Assert.Equal(count, (await StateOf(id)).Count), cancellationToken: TestCancellation);

    protected Task AwaitProbeCalls(int count) =>
        AwaitAssertAsync(() => Assert.Equal(count, World.ProbeCalls.Count), cancellationToken: TestCancellation);

    protected async Task<IActorRef> EntityActorOf(string id) =>
        await Sys.ActorSelection(EntityPath(id)).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);

    protected async Task AwaitNoPending(string id) =>
        await AwaitAssertAsync(async () =>
                Assert.Empty(await PendingEffects<CounterCommand>(id).WaitAsync(TimeSpan.FromSeconds(1), TestCancellation)),
            cancellationToken: TestCancellation);

}

public abstract class RecordedExecuteHostingSpec(ITestOutputHelper output) : ProbeHostingSpec(output)
{
    [Fact]
    public async Task An_answered_effect_is_decided_once_and_is_no_longer_pending()
    {
        Say("answered", "issue:a");

        await AwaitCount("answered", 101); // 100 with the decision, 1 from the answer
        await AwaitNoPending("answered");
        await ExpectNoMsgAsync(NoMsgWindow, TestCancellation);
        Assert.Equal(101, (await StateOf("answered")).Count);
        Assert.Single(World.ProbeCalls);
        Assert.Single(World.Decided, d => d == "answer:a");
    }

    [Theory]
    [InlineData("a,b")]
    [InlineData("b,a")]
    [InlineData("a,a")]
    public async Task Pending_effects_come_back_in_the_order_they_were_issued(string names)
    {
        World.Hold(names.Split(','));

        Say("order", $"issue2:{names}");
        await AwaitProbeCalls(2);

        var pending = await PendingEffects<CounterCommand>("order");
        Assert.Equal(names.Split(',').Select(name => new Probe(name)), pending.Cast<Probe>());
    }

    // The effect timeout of the entity is 500 ms.
    [Fact]
    public async Task A_command_a_reminder_delivers_issues_a_recorded_effect_like_any_other()
    {
        Say("scheduled", "later:a");

        await AwaitCount("scheduled", 101);
        await AwaitNoPending("scheduled");
        var (effect, delivery) = Assert.Single(World.ProbeCalls);
        Assert.Equal(new Probe("a"), effect);
        Assert.StartsWith("counter/scheduled/", delivery.Id.Value);
    }

    [Theory]
    [InlineData("issue:a", "throw", "a:Failed:1:True")]
    [InlineData("issue:a", "hang", "a:TimedOut:1:True")]
    [InlineData("note:a", "throw", "a:Failed:1:True")]
    [InlineData("note:a", "hang", "a:TimedOut:1:True")]
    public async Task A_handler_that_throws_or_hangs_gives_up_and_Decide_gets_that_give_up(
        string command, string how, string expected)
    {
        Task Misbehave(CancellationToken ct) =>
            how == "throw" ? throw new InvalidOperationException("boom") : Task.Delay(Timeout.Infinite, ct);
        World.OnProbe = async (_, ct) =>
        {
            await Misbehave(ct);
            return default;
        };
        World.OnNote = (_, ct) => Misbehave(ct);

        Say("misbehave", command);

        await AwaitAssertAsync(async () => Assert.Equal(expected, (await StateOf("misbehave")).LastGaveUp),
            cancellationToken: TestCancellation);
        await AwaitNoPending("misbehave");
        Assert.Equal(1, World.ProbeCalls.Count + World.NoteCalls.Count);
    }

    [Fact]
    public async Task An_entity_that_stops_with_an_attempt_in_flight_gives_the_effect_up_as_interrupted_when_asked_again()
    {
        World.Hold("a");
        await StateOf("stopped", FirstAskTimeout);
        Say("stopped", "issue:a");
        await AwaitProbeCalls(1);
        var entity = await EntityActorOf("stopped");
        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: TestCancellation);

        // Asking starts the entity again; it gives up what it finds pending in its journal before it answers.
        await AwaitNoPending("stopped");

        Assert.Equal("a:Interrupted:1:True", (await StateOf("stopped")).LastGaveUp);
        World.Release("a"); // the first attempt is over for good
        await ExpectNoMsgAsync(NoMsgWindow, TestCancellation);
        Assert.Equal(new CounterState(100, false, LastGaveUp: "a:Interrupted:1:True"), await StateOf("stopped"));
        Assert.Single(World.ProbeCalls);
    }
}

public sealed class LocalRecordedExecuteHostingSpecs(ITestOutputHelper output) : RecordedExecuteHostingSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    protected override string EntityPath(string id) =>
        $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/{Uri.EscapeDataString(id)}";
}

public sealed class ShardedRecordedExecuteHostingSpecs(ITestOutputHelper output) : RecordedExecuteHostingSpec(output)
{
    protected override ShardedHosting? Sharding => new();

    protected override string EntityPath(string id) => $"/system/sharding/{Counter.TypeName.Value}/*/{id}";
}

/// <summary>Passivation of an idle entity while an attempt is in flight that runs out of time.</summary>
public abstract class RecordedExecutePassivationSpec(ITestOutputHelper output) : ProbeHostingSpec(output)
{
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(1);

    protected override EntityOptions Options =>
        EntityOptions.Default with { PassivateIdleAfter = IdleTimeout, EffectTimeout = TimeSpan.FromSeconds(3) };

    [Fact]
    public async Task A_time_out_that_ends_a_deferred_stop_is_journaled_as_a_give_up_before_the_entity_stops()
    {
        World.OnProbe = async (_, ct) =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return default;
        };
        await StateOf("passivated", FirstAskTimeout);
        Say("passivated", "issue:a");
        await AwaitProbeCalls(1);
        var entity = await EntityActorOf("passivated");
        Watch(entity);

        // The host asks the entity to stop after one idle timeout. It stays up for the attempt, until the attempt times out.
        await ExpectNoMsgAsync(IdleTimeout * 1.5, TestCancellation);
        await ExpectTerminatedAsync(entity, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

        // The new incarnation finds the give-up in the journal. It is not interrupted, and nothing ran again.
        var state = await StateOf("passivated");
        Assert.Equal(new CounterState(100, false, LastGaveUp: "a:TimedOut:1:True"), state);
        Assert.Single(World.ProbeCalls);
        Assert.Single(World.Decided, d => d.StartsWith("gaveup:", StringComparison.Ordinal));
    }
}

public sealed class LocalRecordedExecutePassivationSpecs(ITestOutputHelper output) : RecordedExecutePassivationSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    protected override string EntityPath(string id) =>
        $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/{Uri.EscapeDataString(id)}";
}

public sealed class ShardedRecordedExecutePassivationSpecs(ITestOutputHelper output) : RecordedExecutePassivationSpec(output)
{
    protected override ShardedHosting? Sharding => new();

    protected override string EntityPath(string id) => $"/system/sharding/{Counter.TypeName.Value}/*/{id}";
}

/// <summary>What the host checks about effects when it starts, and what registration refuses.</summary>
public sealed class RecordedExecuteStartupSpecs
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A counter effect with no serializer at all.</summary>
    public sealed record Unserializable(int Value) : IEffect<CounterCommand>
    {
        public CounterCommand GaveUp(WorkGaveUp gaveUp) => new Noop();
    }

    public sealed class UnserializableHandler : IEffectHandler<Unserializable, CounterCommand>
    {
        public Task<CounterCommand> HandleAsync(Unserializable effect, EffectDelivery delivery, CancellationToken ct) =>
            Task.FromResult<CounterCommand>(new Noop());
    }

    /// <summary>Answers with text, which is no command of any entity.</summary>
    public sealed class TextAnswerHandler : IEffectHandler<Probe, string>
    {
        public Task<string> HandleAsync(Probe effect, EffectDelivery delivery, CancellationToken ct) =>
            Task.FromResult("text");
    }

    public sealed class QuietProbeHandler : IEffectHandler<Probe>
    {
        public Task HandleAsync(Probe effect, EffectDelivery delivery, CancellationToken ct) => Task.CompletedTask;
    }

    private static async Task<string?> StartAndStop(Action<IServiceCollection> handlers, bool probeSerializer)
    {
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                handlers(services);
                services.AddAkka("effect-startup", builder =>
                {
                    builder.WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
                        .WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)));
                    if (probeSerializer) builder.WithCustomSerializer(ProbeSerializer.CreateSerializerDetails);
                    builder.WithDurableEntity(Counter.Definition);
                });
            })
            .Build();

        try
        {
            await host.StartAsync(Ct);
            return null;
        }
        catch (Exception ex)
        {
            return ex.ToString();
        }
        finally
        {
            await host.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_route_whose_effect_has_a_serializer_that_is_not_registered_fails_startup_naming_both()
    {
        // Probe is marked for the probe serializer, but the host does not know that serializer: it would be the fallback.
        var failure = await StartAndStop(
            s => s.AddSingleton(new ProbeWorld()).AddEffectHandler<Probe, CounterCommand, ProbeHandler>(), probeSerializer: false);

        Assert.NotNull(failure);
        Assert.Contains("Probe", failure);
        Assert.Contains("ProbeHandler", failure);
    }

    [Fact]
    public async Task A_route_whose_effect_has_no_serializer_fails_startup_also_for_a_handler_with_an_answer()
    {
        var failure = await StartAndStop(
            s => s.AddSingleton(new ProbeWorld()).AddEffectHandler<Unserializable, CounterCommand, UnserializableHandler>(),
            probeSerializer: true);

        Assert.NotNull(failure);
        Assert.Contains("Unserializable", failure);
        Assert.Contains("UnserializableHandler", failure);
    }

    [Fact]
    public async Task A_route_whose_answer_is_not_the_command_type_of_the_entity_fails_startup_naming_both()
    {
        var failure = await StartAndStop(
            s => s.AddSingleton(new ProbeWorld()).AddEffectHandler<Probe, string, TextAnswerHandler>(), probeSerializer: true);

        Assert.NotNull(failure);
        Assert.Contains("Probe", failure);
        Assert.Contains("TextAnswerHandler", failure);
    }

    [Fact]
    public async Task Routes_that_are_right_start_and_the_routes_of_other_entity_types_are_not_checked()
    {
        // Calculate belongs to the railway entity, which this host does not run. Its serializer is not registered here.
        var failure = await StartAndStop(
            s => s.AddProbe(new ProbeWorld()).AddEffectHandler<Calculate, RailwayCommand, CalculateHandler>(),
            probeSerializer: true);

        Assert.Null(failure);
    }

    [Theory]
    [InlineData("answer then no answer")]
    [InlineData("no answer then answer")]
    [InlineData("the same handler twice")]
    public void A_second_route_for_one_effect_type_is_refused_at_registration(string order)
    {
        var services = new ServiceCollection();

        var failure = Assert.Throws<InvalidOperationException>(() =>
        {
            switch (order)
            {
                case "answer then no answer":
                    services.AddEffectHandler<Probe, CounterCommand, ProbeHandler>();
                    services.AddEffectHandler<Probe, QuietProbeHandler>();
                    break;
                case "no answer then answer":
                    services.AddEffectHandler<Probe, QuietProbeHandler>();
                    services.AddEffectHandler<Probe, CounterCommand, ProbeHandler>();
                    break;
                default:
                    services.AddEffectHandler<Probe, CounterCommand, ProbeHandler>();
                    services.AddEffectHandler<Probe, CounterCommand, ProbeHandler>();
                    break;
            }
        });

        Assert.Contains("Probe", failure.Message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_handler_is_registered_as_scoped_so_each_attempt_gets_its_own(bool withAnswer)
    {
        var services = new ServiceCollection().AddSingleton(new ProbeWorld());
        if (withAnswer) services.AddEffectHandler<Probe, CounterCommand, ProbeHandler>();
        else services.AddEffectHandler<Probe, QuietProbeHandler>();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        object Resolve(IServiceProvider scope) => withAnswer
            ? scope.GetRequiredService<IEffectHandler<Probe, CounterCommand>>()
            : scope.GetRequiredService<IEffectHandler<Probe>>();

        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        Assert.Same(Resolve(first.ServiceProvider), Resolve(first.ServiceProvider));
        Assert.NotSame(Resolve(first.ServiceProvider), Resolve(second.ServiceProvider));
        Assert.IsType(withAnswer ? typeof(ProbeHandler) : typeof(QuietProbeHandler), Resolve(first.ServiceProvider));
        Assert.Throws<InvalidOperationException>(() => Resolve(provider)); // not from the root
    }
}

/// <summary>Events and records of the library in one batch, written to SQLite and read back by new actor systems.</summary>
public sealed class RecordedExecuteSqlMixedSpecs : IDisposable
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-{Guid.NewGuid():N}.db");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static CancellationToken AskTimeout(int seconds = 30)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(seconds));
        return cts.Token;
    }

    private async Task<IHost> StartHost(ProbeWorld world, EntityOptions options)
    {
        var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>().AddCounterHandlers();
                services.AddProbe(world);
                services.AddAkka("recorded-execute-sql-mixed", builder => builder
                    .WithSqlPersistence(connectionString: $"Data Source={_databasePath}", providerName: ProviderName.SQLiteMS)
                    .WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)))
                    .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
                    .WithCustomSerializer(ProbeSerializer.CreateSerializerDetails)
                    .WithDurableEntity(ProbeEntity.Definition(world, options)));
            })
            .Build();

        await host.StartAsync(Ct);
        return host;
    }

    private static IDurableEntityRef<CounterCommand, CounterState, CounterEvent> CounterRef(IHost host) =>
        host.Services.GetRequiredService<IDurableEntityRefs<CounterCommand, CounterState, CounterEvent>>()
            .Get(new EntityId("a"));

    private static async Task<CounterState> StateOf(IHost host) =>
        (await CounterRef(host).GetStateAsync(AskTimeout())).State;

    private static async Task AwaitState(IHost host, Func<CounterState, bool> done)
    {
        while (true)
        {
            if (done(await StateOf(host))) return;
            await Task.Delay(20, Ct);
        }
    }

    private void Execute(string sql)
    {
        using var connection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={_databasePath}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    // 100: recovery reads every record from the journal. 1: a snapshot after each write carries what is pending.
    [Theory]
    [InlineData(100)]
    [InlineData(1)]
    public async Task Events_acknowledgements_give_ups_and_effects_written_together_are_read_back_by_new_systems(int snapshotEvery)
    {
        var options = EntityOptions.Default with { SnapshotEvery = snapshotEvery };
        var first = new ProbeWorld();
        first.Hold("b");

        using (var writer = await StartHost(first, options))
        {
            // Two effects in one write. "a" is answered with an event: one write with an event and an acknowledgement.
            // "b" is still pending when the host stops.
            CounterRef(writer).Tell(new NoHandler("issue2:a,b"));
            await AwaitState(writer, s => s.Count == 1);
            await writer.StopAsync(Ct);
        }

        // The second system reads the batch back: "a" is answered, "b" is interrupted and decided. That decision is a
        // write with a give-up record and an event.
        var second = new ProbeWorld();
        using (var reader = await StartHost(second, options))
        {
            Assert.Equal(new CounterState(1, false, LastGaveUp: "b:Interrupted:1:True"), await StateOf(reader));
            Assert.Empty(second.ProbeCalls);
            await reader.StopAsync(Ct);
        }

        // The third system reads the give-up back: nothing is pending and nothing is decided again.
        var third = new ProbeWorld();
        using var last = await StartHost(third, options);
        Assert.Equal(new CounterState(1, false, LastGaveUp: "b:Interrupted:1:True"), await StateOf(last));
        Assert.Empty(third.Decided);
        Assert.Empty(third.ProbeCalls);
        await last.StopAsync(Ct);
    }

    [Fact]
    public async Task A_store_that_refuses_the_effect_record_keeps_the_events_of_the_same_decision_out_too()
    {
        var world = new ProbeWorld();
        world.Hold("a");
        using var host = await StartHost(world, EntityOptions.Default);
        await CounterRef(host).Ask(new Increment(1), AskTimeout());

        // The journal refuses the row of the effect (the third row: the event of its decision is the second). The
        // event and the effect are one write, so the refusal takes the event with it.
        Execute("""
            CREATE TRIGGER refuse_effect BEFORE INSERT ON journal WHEN NEW.sequence_number = 3
            BEGIN SELECT RAISE(ABORT, 'refused'); END;
            """);
        CounterRef(host).Tell(new NoHandler("issue:a"));
        while (!world.Decided.Contains("issue:a")) await Task.Delay(20, Ct);

        // The trigger stays: the write can reach the journal after the decision, so dropping it here would race. Nothing
        // later in this spec writes the third row. Whatever the entity does next, no incarnation sees the event, and no
        // handler ran.
        await host.StopAsync(Ct);
        var second = new ProbeWorld();
        using var reader = await StartHost(second, EntityOptions.Default);
        Assert.Equal(new CounterState(1, false), await StateOf(reader));
        Assert.Empty(world.ProbeCalls);
        Assert.Empty(second.Decided);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }
}
