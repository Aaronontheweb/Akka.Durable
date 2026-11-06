using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Abstractions.Entities;
using Akka.Durable.Entities.Tests.Fixtures;
using Akka.Event;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Entities.Tests;

/// <summary>
/// EntityActor running against a real hosted ActorSystem and the Akka.Persistence TestKit journal.
/// Each test gets a fresh host, journal, and singleton services.
/// </summary>
public class EntityActorSpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private static readonly TimeSpan NoMsgWindow = TimeSpan.FromMilliseconds(300);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
    }

    private EffectRecorder Recorder => Host.Services.GetRequiredService<EffectRecorder>();

    private Props EntityProps(
        string entityId,
        EntityDefinition<CounterState, CounterCommand, CounterEvent>? definition = null)
    {
        var id = new EntityId(entityId);
        var def = definition ?? Counter.Definition;
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Props.Create(() => new EntityActor<CounterState, CounterCommand, CounterEvent>(id, def, services, time));
    }

    private IActorRef SpawnEntity(
        string entityId,
        EntityDefinition<CounterState, CounterCommand, CounterEvent>? definition = null) =>
        Sys.ActorOf(EntityProps(entityId, definition));

    // Parameter typed as the union so case records are converted (and boxed) as CounterCommand.
    private void Send(IActorRef entity, CounterCommand command, IActorRef? sender = null) =>
        entity.Tell(command, sender ?? TestActor);

    private async Task<object[]> ReadJournal(string persistenceId)
    {
        var probe = CreateTestProbe();
        Sys.ActorOf(Props.Create(() => new JournalReader(persistenceId, probe.Ref)));
        return await probe.ExpectMsgAsync<object[]>(cancellationToken: Ct);
    }

    #region Persist -> Evolve -> effects

    [Fact]
    public async Task Persisted_command_replies_after_commit_and_updates_state()
    {
        var entity = SpawnEntity("c1");

        Send(entity, new Increment(5));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Send(entity, new GetCount());
        await ExpectMsgAsync<CountIs>(c => c.Count == 5, cancellationToken: Ct);
    }

    [Fact]
    public async Task Failed_journal_write_runs_no_effects_and_stops_the_entity()
    {
        var entity = SpawnEntity("c2");
        Watch(entity);

        await WithJournalWrite(write => write.Fail(), async () =>
        {
            Send(entity, new IncrementAndRecord(1));

            // Terminated must be the next message: no Accepted reply may precede it.
            await ExpectTerminatedAsync(entity, cancellationToken: Ct);
        });

        Assert.Empty(Recorder.Entries);
    }

    [Fact]
    public async Task Recovery_replays_events_through_Evolve_but_never_reruns_effects()
    {
        var entity = SpawnEntity("c3");
        Send(entity, new IncrementAndRecord(3));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Send(entity, new Increment(4));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);

        var recovered = SpawnEntity("c3");
        Send(recovered, new GetCount());
        await ExpectMsgAsync<CountIs>(c => c.Count == 7, cancellationToken: Ct);

        Assert.Equal(["run:3", "disposed"], Recorder.Entries);
    }

    #endregion

    #region Persistence ids

    [Fact]
    public async Task Entity_writes_to_type_name_dash_entity_id()
    {
        var entity = SpawnEntity("abc");
        Send(entity, new Increment(2));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        var stored = await ReadJournal("counter-abc");

        var only = Assert.Single(stored);
        Assert.Equal((CounterEvent)new Incremented(2), only);
    }

    [Fact]
    public async Task Entity_honours_a_custom_persistence_id_factory()
    {
        var definition = Counter.Definition with
        {
            Options = EntityOptions.Default.WithPersistenceId((id, type) => $"custom/{type.Value}/{id.Value}")
        };

        var entity = SpawnEntity("xyz", definition);
        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Assert.Single(await ReadJournal("custom/counter/xyz"));
        Assert.Empty(await ReadJournal("counter-xyz"));
    }

    #endregion

    #region Decision algebra

    [Fact]
    public async Task Ignored_command_is_silent()
    {
        var unhandled = CreateTestProbe();
        Sys.EventStream.Subscribe(unhandled.Ref, typeof(UnhandledMessage));
        var entity = SpawnEntity("c4");

        Send(entity, new Noop());

        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        await unhandled.ExpectNoMsgAsync(NoMsgWindow, Ct);
    }

    [Fact]
    public async Task Unhandled_command_goes_through_Akka_unhandled_handling()
    {
        var unhandled = CreateTestProbe();
        Sys.EventStream.Subscribe(unhandled.Ref, typeof(UnhandledMessage));
        var entity = SpawnEntity("c5");

        Send(entity, new Unsupported());

        await unhandled.ExpectMsgAsync<UnhandledMessage>(
            m => m.Message is CounterCommand { Value: Unsupported }, cancellationToken: Ct);
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
    }

    [Fact]
    public async Task Stashed_command_keeps_its_sender_and_runs_after_UnstashAll()
    {
        var entity = SpawnEntity("c6");
        var otherSender = CreateTestProbe();

        Send(entity, new LockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Send(entity, new Increment(2), otherSender.Ref);
        await otherSender.ExpectNoMsgAsync(NoMsgWindow, Ct);

        Send(entity, new UnlockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        await otherSender.ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Send(entity, new GetCount());
        await ExpectMsgAsync<CountIs>(c => c.Count == 2, cancellationToken: Ct);
    }

    #endregion

    #region Run effects

    [Fact]
    public async Task Zero_event_decision_holds_the_mailbox_until_effects_complete()
    {
        var entity = SpawnEntity("c7");

        Send(entity, new WaitForGate());
        Send(entity, new GetCount());

        // GetCount must not be processed while the Run effect is outstanding.
        await ExpectNoMsgAsync(NoMsgWindow, Ct);

        Host.Services.GetRequiredService<Gate>().Open();
        await ExpectMsgAsync<GateOpened>(cancellationToken: Ct);
        await ExpectMsgAsync<CountIs>(c => c.Count == 0, cancellationToken: Ct);
    }

    [Fact]
    public async Task Run_resolves_a_scoped_service_and_disposes_the_scope_before_the_next_effect()
    {
        var entity = SpawnEntity("c8");

        Send(entity, new IncrementAndRecord(2));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        // Reply is declared after Run, so by the time it arrives the scope is gone.
        Assert.Equal(["run:2", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Run_failure_without_OnFailure_escalates_to_supervision()
    {
        var entity = SpawnEntity("c9");

        await EventFilter.Exception<InvalidOperationException>("boom").ExpectOneAsync(() =>
        {
            Send(entity, new IncrementThenFail(1));
            return Task.CompletedTask;
        }, Ct);

        // The Reply after the failed Run never happens...
        await ExpectNoMsgAsync(NoMsgWindow, Ct);

        // ...but the event committed before the effect ran, and the restarted actor recovers it.
        Send(entity, new GetCount());
        await ExpectMsgAsync<CountIs>(c => c.Count == 1, cancellationToken: Ct);
        Assert.Equal(["disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Run_failure_with_OnFailure_feeds_a_command_back_through_Decide()
    {
        var entity = SpawnEntity("c10");

        Send(entity, new FailWithFallback());

        await ExpectMsgAsync<FailureRecorded>(f => f.Reason == "boom", cancellationToken: Ct);
    }

    [Fact]
    public async Task Run_timeout_is_reported_as_a_TimeoutException()
    {
        var entity = SpawnEntity("c11");

        Send(entity, new WaitForGateWithTimeout(TimeSpan.FromMilliseconds(200)));

        await ExpectMsgAsync<FailureRecorded>(f => f.Reason == nameof(TimeoutException), cancellationToken: Ct);
    }

    #endregion

    #region Lifecycle

    [Fact]
    public async Task Stop_passivates_through_the_parent_after_preceding_effects()
    {
        var parentProbe = CreateTestProbe();
        var entityProps = EntityProps("c12");
        var parent = Sys.ActorOf(Props.Create(() => new TestEntityParent(entityProps, parentProbe.Ref)));

        Send(parent, new Shutdown());

        // Reply is declared before Stop, so it is delivered first.
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        await parentProbe.ExpectMsgAsync<Passivate>(cancellationToken: Ct);
        var passivatingEntity = parentProbe.LastSender;

        var stopped = await parentProbe.ExpectMsgAsync<EntityStopped>(cancellationToken: Ct);
        Assert.Equal(stopped.Entity, passivatingEntity);
    }

    #endregion
}
