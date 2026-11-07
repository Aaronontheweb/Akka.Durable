using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Abstractions.Entities;
using Akka.Durable.Entities.Tests.Fixtures;
using Akka.Hosting.TestKit;
using Akka.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Entities.Tests;

/// <summary>
/// Recovery must fail loudly when the journal or snapshot store holds data the entity cannot interpret,
/// rather than recovering with silently missing state.
/// </summary>
public class RecoverySpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private static readonly TimeSpan NoMsgWindow = TimeSpan.FromMilliseconds(300);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
    }

    private IActorRef SpawnEntity(
        string entityId,
        EntityDefinition<CounterState, CounterCommand, CounterEvent>? definition = null)
    {
        var id = new EntityId(entityId);
        var def = definition ?? Counter.Definition;
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Sys.ActorOf(Props.Create(() =>
            new EntityActor<CounterState, CounterCommand, CounterEvent>(id, def, services, time)));
    }

    // Plants arbitrary journal entries / snapshots under the entity's persistence id.
    private async Task Plant(string entityId, params object[] events) =>
        await PlantCore(entityId, snapshot: null, events);

    private async Task PlantCore(string entityId, object? snapshot, object[] events)
    {
        var planter = Sys.ActorOf(Props.Create(() => new Planter($"counter-{entityId}")));
        if (snapshot is not null)
        {
            planter.Tell(new Planter.SaveSnap(snapshot), TestActor);
            await ExpectMsgAsync<Planter.Done>(cancellationToken: Ct);
        }

        foreach (var e in events)
        {
            planter.Tell(new Planter.Write(e), TestActor);
            await ExpectMsgAsync<Planter.Done>(cancellationToken: Ct);
        }

        Watch(planter);
        Sys.Stop(planter);
        await ExpectTerminatedAsync(planter, cancellationToken: Ct);
    }

    private async Task AssertRecoveryFails(string entityId, string expectedInError)
    {
        IActorRef entity = null!;

        // Recovery starts at spawn, so the filter must cover the spawn too.
        await EventFilter.Exception<EntityRecoveryException>(contains: expectedInError).ExpectOneAsync(() =>
        {
            entity = SpawnEntity(entityId);
            Watch(entity);
            entity.Tell((CounterCommand)new GetCount(), TestActor);
            return Task.CompletedTask;
        }, Ct);

        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
    }

    [Fact]
    public async Task Foreign_journal_entry_fails_recovery()
    {
        await Plant("r1", "not-an-event");

        await AssertRecoveryFails("r1", typeof(string).ToString());
    }

    [Fact]
    public async Task Event_union_case_persisted_unboxed_fails_recovery()
    {
        // Simulates a serializer round-trip that returns the case type instead of the boxed union.
        await Plant("r2", new Incremented(1));

        await AssertRecoveryFails("r2", typeof(Incremented).ToString());
    }

    [Fact]
    public async Task Incompatible_snapshot_fails_recovery()
    {
        await PlantCore("r3", snapshot: "bogus", [(object)(CounterEvent)new Incremented(1)]);

        await AssertRecoveryFails("r3", typeof(CounterState).ToString());
    }

    [Fact]
    public async Task Compatible_snapshot_and_union_events_recover_normally()
    {
        await PlantCore("r4", new CounterState(5, false), [(object)(CounterEvent)new Incremented(2)]);

        var entity = SpawnEntity("r4");
        entity.Tell((CounterCommand)new GetCount(), TestActor);
        await ExpectMsgAsync<CountIs>(c => c.Count == 7, cancellationToken: Ct);
    }

    [Fact]
    public async Task Unrecognized_entries_can_be_skipped_by_opting_in()
    {
        await Plant("r5", (CounterEvent)new Incremented(2), "not-an-event", (CounterEvent)new Incremented(3));
        var skipping = Counter.Definition with
        {
            Options = EntityOptions.Default with { SkipUnrecognizedJournalEntries = true },
        };

        // The skipped entry is still reported as an error; the valid events around it are applied.
        IActorRef entity = null!;
        await EventFilter.Error(contains: typeof(string).ToString()).ExpectOneAsync(() =>
        {
            entity = SpawnEntity("r5", skipping);
            entity.Tell((CounterCommand)new GetCount(), TestActor);
            return Task.CompletedTask;
        }, Ct);

        await ExpectMsgAsync<CountIs>(c => c.Count == 5, cancellationToken: Ct);
    }

    private sealed class Planter : ReceivePersistentActor
    {
        public sealed record Write(object Payload);
        public sealed record SaveSnap(object Snapshot);
        public sealed record Done;

        public Planter(string persistenceId)
        {
            PersistenceId = persistenceId;
            Recover<object>(_ => { });
            Command<Write>(w =>
            {
                var sender = Sender;
                Persist(w.Payload, _ => sender.Tell(new Done()));
            });
            Command<SaveSnap>(s =>
            {
                var sender = Sender;
                SaveSnapshot(s.Snapshot);
                BecomeStacked(() => Command<SaveSnapshotSuccess>(_ =>
                {
                    sender.Tell(new Done());
                    UnbecomeStacked();
                }));
            });
        }

        public override string PersistenceId { get; }
    }
}
