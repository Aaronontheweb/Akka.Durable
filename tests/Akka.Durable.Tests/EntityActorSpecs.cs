using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures;
using Akka.Event;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

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
        services.AddCounterHandlers();
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
    }

    private EffectRecorder Recorder => Host.Services.GetRequiredService<EffectRecorder>();

    private Gate Gate => Host.Services.GetRequiredService<Gate>();

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

    private async Task<int> CountOf(IActorRef entity)
    {
        Send(entity, new GetCount());
        return (await ExpectMsgAsync<CountIs>(cancellationToken: Ct)).Count;
    }

    private Task AssertCountBecomes(IActorRef entity, int expected) =>
        AwaitAssertAsync(async () => Assert.Equal(expected, await CountOf(entity)), cancellationToken: Ct);

    #region Persist -> Evolve -> effects

    [Fact]
    public async Task Persisted_command_replies_after_commit_and_updates_state()
    {
        var entity = SpawnEntity("c1");

        Send(entity, new Increment(5));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Assert.Equal(5, await CountOf(entity));
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
        Assert.Equal(7, await CountOf(recovered));

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
    public async Task State_query_bypasses_the_decider()
    {
        var decided = new List<CounterCommand>();
        var definition = Counter.Definition with
        {
            Decide = (state, command, ctx) =>
            {
                decided.Add(command);
                return Counter.Decide(state, command, ctx);
            },
        };
        var entity = SpawnEntity("c-query", definition);

        Send(entity, new Increment(2));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        entity.Tell(GetEntityState.Instance, TestActor);

        var reply = await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct);
        Assert.Equal(new CounterState(2, false), reply.State);
        Assert.Equal(1, reply.SequenceNr);
        Assert.Single(decided);
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

        Assert.Equal(2, await CountOf(entity));
    }

    #endregion

    #region Run effects

    [Fact]
    public async Task Zero_event_decision_holds_the_mailbox_until_Run_effects_complete()
    {
        var entity = SpawnEntity("c7");

        Send(entity, new WaitForGate());
        Send(entity, new GetCount());

        // GetCount must not be processed while the Run effect is outstanding.
        await ExpectNoMsgAsync(NoMsgWindow, Ct);

        Gate.Open();
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
    public async Task Run_failure_is_logged_and_later_effects_still_run()
    {
        var entity = SpawnEntity("c9");

        await EventFilter.Custom<Warning>(warning => warning.Cause is InvalidOperationException { Message: "boom" })
            .ExpectOneAsync(async () =>
            {
                Send(entity, new IncrementThenFail(1));
                await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
            }, Ct);

        // The event committed before the effect ran, and the entity was not restarted.
        Assert.Equal(1, await CountOf(entity));
        Assert.Equal(["disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Run_timeout_is_logged_and_later_effects_still_run()
    {
        var entity = SpawnEntity("c11");

        await EventFilter.Custom<Warning>(warning => warning.Cause is TimeoutException)
            .ExpectOneAsync(async () =>
            {
                Send(entity, new WaitForGateWithTimeout(TimeSpan.FromMilliseconds(200)));
                await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
            }, Ct);
    }

    #endregion

    #region Execute effects

    [Fact]
    public async Task Effect_outcome_is_decided_and_replies_to_the_asker_of_the_command_that_issued_it()
    {
        var entity = SpawnEntity("e1");

        Send(entity, new IncrementViaService(2, Wait: false));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Assert.Equal(4, await CountOf(entity));
        Assert.Equal(["compute:2", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Each_outcome_answers_its_own_asker()
    {
        var entity = SpawnEntity("e2");
        var first = CreateTestProbe();
        var second = CreateTestProbe();

        Send(entity, new IncrementViaService(1, Wait: false), first.Ref);
        Send(entity, new IncrementViaService(2, Wait: false), second.Ref); // stashed until the first outcome lands

        await first.ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        await second.ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Assert.Equal(6, await CountOf(entity));
    }

    [Fact]
    public async Task Entity_keeps_handling_commands_while_an_effect_is_in_flight()
    {
        var entity = SpawnEntity("e3");

        Send(entity, new IncrementViaService(3, Wait: true));
        Assert.Equal(0, await CountOf(entity)); // answered while the handler waits on the gate

        Gate.Open();
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Assert.Equal(6, await CountOf(entity));
    }

    [Fact]
    public async Task Failed_effect_is_logged_and_the_entity_keeps_running()
    {
        var entity = SpawnEntity("e5");

        await EventFilter.Custom<Warning>(warning => warning.Cause is InvalidOperationException { Message: "boom" })
            .ExpectOneAsync(async () =>
            {
                Send(entity, new ExplodeLater());
                await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
            }, Ct);

        Assert.Equal(0, await CountOf(entity));
    }

    [Fact]
    public async Task Missing_effect_handler_is_a_configuration_error_that_restarts_the_entity()
    {
        var entity = SpawnEntity("e6");

        await EventFilter.Exception<InvalidOperationException>(contains: "No effect handler is registered")
            .ExpectOneAsync(() =>
            {
                Send(entity, new NoHandler());
                return Task.CompletedTask;
            }, Ct);

        Assert.Equal(0, await CountOf(entity));
    }

    #endregion

    #region Resume

    [Fact]
    public async Task Resume_re_issues_the_effect_a_recovered_state_says_is_pending()
    {
        var entity = SpawnEntity("r1");
        Send(entity, new IncrementViaService(2, Wait: true));
        await AwaitAssertAsync(() => Assert.Equal(["compute:2"], Recorder.Entries), cancellationToken: Ct);

        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);

        // The request is in the journal, so the next incarnation asks the service again.
        var recovered = SpawnEntity("r1");
        await AwaitAssertAsync(() => Assert.Equal(2, Recorder.Entries.Count(e => e == "compute:2")), cancellationToken: Ct);

        // Both handler calls finish. The first incarnation's outcome is dropped; the second's is applied.
        Gate.Open();
        await AssertCountBecomes(recovered, 4);
        await ExpectNoMsgAsync(NoMsgWindow, Ct); // the original asker belongs to the old incarnation
    }

    [Fact]
    public async Task Resume_re_issues_the_effect_when_the_state_comes_from_a_snapshot_alone()
    {
        // A snapshot after every event, and the events behind it deleted: a compacted journal.
        var definition = Counter.Definition with
        {
            Options = EntityOptions.Default with { SnapshotEvery = 1, KeepSnapshots = 1, DeleteEventsOnSnapshot = true },
        };
        var entity = SpawnEntity("r3", definition);
        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Send(entity, new IncrementViaService(2, Wait: true));
        await AwaitAssertAsync(() => Assert.Equal(["compute:2"], Recorder.Entries), cancellationToken: Ct);

        // The latest snapshot covers every event, so a recovery replays none of them.
        await AwaitAssertAsync(async () => Assert.Empty(await ReadJournal("counter-r3")), cancellationToken: Ct);

        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);

        // The pending request comes back in the snapshot, and Resume needs nothing else.
        var recovered = SpawnEntity("r3", definition);
        await AwaitAssertAsync(() => Assert.Equal(2, Recorder.Entries.Count(e => e == "compute:2")), cancellationToken: Ct);

        Gate.Open();
        await AssertCountBecomes(recovered, 5);
    }

    [Fact]
    public async Task Resume_may_not_persist_events()
    {
        var definition = Counter.Definition with
        {
            Resume = (_, ctx) => ctx.Persist(new Incremented(1)),
        };
        var probe = CreateTestProbe();
        var props = EntityProps("r2", definition);

        await EventFilter.Exception<InvalidOperationException>(contains: "Resume returned events").ExpectOneAsync(() =>
        {
            Sys.ActorOf(Props.Create(() => new TestEntityParent(props, probe.Ref, true)));
            return Task.CompletedTask;
        }, Ct);

        await probe.ExpectMsgAsync<EntityStopped>(cancellationToken: Ct);
    }

    #endregion

    #region Accepted messages

    [Fact]
    public async Task A_message_the_definition_accepts_becomes_a_command_from_its_sender()
    {
        var entity = SpawnEntity("a1", Counter.Definition.Accepts<string>(text => new Increment(text.Length)));

        entity.Tell("abcd", TestActor);

        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Assert.Equal(4, await CountOf(entity));
    }

    [Fact]
    public async Task A_message_the_definition_does_not_accept_is_unhandled()
    {
        var unhandled = CreateTestProbe();
        Sys.EventStream.Subscribe(unhandled.Ref, typeof(UnhandledMessage));
        var entity = SpawnEntity("a2");

        entity.Tell(42.0, TestActor);

        await unhandled.ExpectMsgAsync<UnhandledMessage>(m => m.Message is 42.0, cancellationToken: Ct);
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
    }

    #endregion

    #region Lifecycle

    [Fact]
    public async Task Stop_passivates_through_the_parent_after_preceding_effects()
    {
        var parentProbe = CreateTestProbe();
        var entityProps = EntityProps("c12");
        var parent = Sys.ActorOf(Props.Create(() => new TestEntityParent(entityProps, parentProbe.Ref, false)));

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
