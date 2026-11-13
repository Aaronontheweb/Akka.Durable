using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Persistence;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// Effects that were journaled while an entity's effect records still implemented <c>IEffect&lt;TCommand&gt;</c>, read
/// and given up by the code that has no such interface. The interface was never part of the stored form: an effect is
/// stored through its own serializer and manifest.
/// </summary>
/// <remarks>
/// <para>The bytes below were written by commit <c>e63c7be</c> of <c>dev</c>, the last one before the interface was
/// removed, when <c>Compute</c>, <c>Probe</c> and <c>Announcement</c> implemented <c>IEffect&lt;CounterCommand&gt;</c>.
/// Each is <c>system.Serialization.FindSerializerFor(value).ToBinary(value)</c> in an actor system with
/// <c>CounterSerializer</c>, <c>ProbeSerializer</c> and <c>DurableEntitySerializer</c>, for the value that
/// <see cref="Current"/> builds under the same name. All were written by the library's serializer, id 22560.</para>
/// <para>The test journal keeps objects, not bytes. So a spec reads a record from its old bytes and puts it in the
/// journal of an entity, where the old code would have left it.</para>
/// </remarks>
public sealed class StoredEffectBytesSpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private const int LibrarySerializer = 22560;
    private const string IssuedManifest = "durable-effect-issued-v1";
    private const string SnapshotManifest = "durable-entity-snapshot-v1";

    private const string IssuedComputeNoPolicy =
        "84008301CD584702B2636F756E7465722D636F6D707574652D763103C407810082000301C20192CF08DE79ABBE2A49800002C00392CF08DE79ABD306DD0000";

    private const string IssuedProbeNoPolicy =
        "84008301CD585102AF70726F62652D6566666563742D763103C4048100A1610192CF08DE79ABBE2A49800002C00392CF08DE79ABD306DD0000";

    private const string IssuedProbePolicy3 =
        "84008301CD585102AF70726F62652D6566666563742D763103C4048100A1610192CF08DE79ABBE2A498000028300CE0098968001CE02FAF08002030392CF08DE79ABD0A4830000";

    private const string IssuedAnnouncementNoPolicy =
        "84008301CD584702B7636F756E7465722D616E6E6F756E63656D656E742D763103C4088100A568656C6C6F0192CF08DE79ABBE2A49800002C00392CF08DE79ABD306DD0000";

    private const string SnapshotPendingCompute =
        "82008301CD584702B0636F756E7465722D73746174652D763103C40D84000201C20282000301C203C001918301CD582002B964757261626C652D70656E64696E672D6566666563742D763103C443860004018301CD584702B2636F756E7465722D636F6D707574652D763103C407810082000301C20292CF08DE79ABBE2A49800003C004010592CF08DE79ABD306DD0000";

    private static readonly DateTimeOffset IssuedAt = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);
    private static readonly IncrementViaService Request = new(3, Wait: false);

    private static (string Manifest, string Hex) Old(string name) => name switch
    {
        "issued-compute-no-policy" => (IssuedManifest, IssuedComputeNoPolicy),
        "issued-probe-no-policy" => (IssuedManifest, IssuedProbeNoPolicy),
        "issued-probe-policy-3" => (IssuedManifest, IssuedProbePolicy3),
        "issued-announcement-no-policy" => (IssuedManifest, IssuedAnnouncementNoPolicy),
        "snapshot-pending-compute" => (SnapshotManifest, SnapshotPendingCompute),
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, null),
    };

    /// <summary>The value the old code serialized under this name, built by the current code.</summary>
    private static object Current(string name) => name switch
    {
        "issued-compute-no-policy" => new EffectIssued(new Compute(Request), IssuedAt, null, IssuedAt.AddSeconds(35)),
        "issued-probe-no-policy" => new EffectIssued(new Probe("a"), IssuedAt, null, IssuedAt.AddSeconds(35)),
        "issued-probe-policy-3" => new EffectIssued(new Probe("a"), IssuedAt,
            new StoredPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), 3), IssuedAt.AddSeconds(31)),
        "issued-announcement-no-policy" => new EffectIssued(new Announcement("hello"), IssuedAt, null, IssuedAt.AddSeconds(35)),
        "snapshot-pending-compute" => new EntitySnapshot(
            new CounterState(2, false, Request),
            [new PendingEffect(4, new Compute(Request), IssuedAt, null, 1, IssuedAt.AddSeconds(35))]),
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, null),
    };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly ProbeWorld _world = new();

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddCounterHandlers()
            .AddProbe(_world);

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
        builder
            .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
            .WithCustomSerializer(ProbeSerializer.CreateSerializerDetails)
            .WithCustomSerializer(DurableEntitySerializer.CreateSerializerDetails)
            .WithLocalReminders(reminders => reminders
                .WithStorage(_ => new InMemoryReminderStorage())
                .WithResolver(system => new DirectEntityReminderResolver(system)));
    }

    #region Helpers

    /// <summary>The record the current code reads from the old bytes.</summary>
    private object Read(string name)
    {
        var (manifest, hex) = Old(name);
        return Sys.Serialization.Deserialize(Convert.FromHexString(hex), LibrarySerializer, manifest);
    }

    private EffectRecorder Recorder => Host.Services.GetRequiredService<EffectRecorder>();

    private IActorRef Spawn(string id, EntityDefinition<CounterState, CounterCommand, CounterEvent> definition)
    {
        var entityId = new EntityId(id);
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Sys.ActorOf(Props.Create(() =>
            new EntityActor<CounterState, CounterCommand, CounterEvent>(entityId, definition, services, time)));
    }

    private async Task<CounterState> StateOf(IActorRef entity)
    {
        entity.Tell(GetEntityState.Instance, TestActor);
        return (CounterState)(await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct)).State;
    }

    private async Task<object[]> PendingOf(IActorRef entity)
    {
        entity.Tell(GetPendingEffects.Instance, TestActor);
        return [.. (await ExpectMsgAsync<PendingEffectsReply>(cancellationToken: Ct)).Effects];
    }

    private async Task Kill(IActorRef actor)
    {
        Watch(actor);
        actor.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(actor, cancellationToken: Ct);
    }

    private static object Event(CounterEvent @event) => @event;

    /// <summary>Writes the entries, and then the snapshot when one is given, under the persistence id of an entity.</summary>
    private async Task WriteJournal(string persistenceId, object[] entries, object? snapshot = null)
    {
        var writer = Sys.ActorOf(Props.Create(() => new OldJournal(persistenceId)));
        writer.Tell(new OldJournal.Append(entries), TestActor);
        await ExpectMsgAsync<OldJournal.Done>(cancellationToken: Ct);
        if (snapshot is not null)
        {
            writer.Tell(new OldJournal.SnapshotOf(snapshot), TestActor);
            await ExpectMsgAsync<OldJournal.Done>(cancellationToken: Ct);
        }

        await Kill(writer);
    }

    #endregion

    public static TheoryData<string> Names =>
    [
        "issued-compute-no-policy", "issued-probe-no-policy", "issued-probe-policy-3", "issued-announcement-no-policy",
        "snapshot-pending-compute",
    ];

    [Theory]
    [MemberData(nameof(Names))]
    public void The_current_code_writes_the_same_bytes_as_the_code_before_the_change(string name)
    {
        var (manifest, hex) = Old(name);
        var value = Current(name);

        var serializer = Sys.Serialization.FindSerializerFor(value);

        Assert.Equal(LibrarySerializer, serializer.Identifier);
        Assert.Equal(manifest, Akka.Serialization.Serialization.ManifestFor(serializer, value));
        Assert.Equal(hex, Convert.ToHexString(serializer.ToBinary(value)));
    }

    [Theory]
    [InlineData("issued-compute-no-policy")]
    [InlineData("issued-probe-no-policy")]
    [InlineData("issued-probe-policy-3")]
    [InlineData("issued-announcement-no-policy")]
    public void An_effect_record_written_before_the_change_is_read_with_its_effect_by_value(string name)
    {
        var issued = Assert.IsType<EffectIssued>(Read(name));

        Assert.Equal(Current(name), issued);
    }

    [Fact]
    public async Task An_effect_journaled_before_the_change_and_never_answered_is_given_up_as_interrupted_through_the_definitions_GaveUp()
    {
        // What the old code left for a service increment whose answer was never journaled: the event of the decision,
        // and the effect in the same write.
        var issued = Assert.IsType<EffectIssued>(Read("issued-compute-no-policy"));
        await WriteJournal("counter-old-compute", [Event(new ServiceIncrementStarted(Request)), issued]);

        var entity = Spawn("old-compute", Counter.Definition);
        var state = await StateOf(entity);

        // The counter's GaveUp makes a ComputeGaveUp of the stored Compute, and Decide ends the service increment.
        Assert.Equal(new CounterState(0, false, Pending: null, LastGaveUp: "compute:Interrupted"), state);
        Assert.Empty(await PendingOf(entity));
        Assert.DoesNotContain("compute:3", Recorder.Entries); // not run again

        // Settled for good: the next incarnation has nothing to give up.
        await Kill(entity);
        var next = Spawn("old-compute", Counter.Definition);
        Assert.Equal(state, await StateOf(next));
        Assert.Empty(await PendingOf(next));
    }

    [Theory]
    [InlineData("issued-probe-no-policy", "a:Interrupted:1:True")]
    [InlineData("issued-announcement-no-policy", "announce:Interrupted")]
    public async Task Each_effect_type_journaled_before_the_change_reaches_Decide_as_its_own_give_up_command(
        string name, string expected)
    {
        await WriteJournal("counter-old-effect", [Read(name)]);

        var entity = Spawn("old-effect", ProbeEntity.Definition(_world));
        var state = await StateOf(entity);

        Assert.Equal(new CounterState(0, false, LastGaveUp: expected), state);
        Assert.Equal([$"gaveup:{expected}"], _world.DecidedNow); // the one command Decide received
        Assert.Empty(await PendingOf(entity));
        Assert.Empty(_world.ProbeCalls);
        Assert.DoesNotContain(Recorder.Entries, entry => entry.StartsWith("announce:", StringComparison.Ordinal));
    }

    [Fact]
    public async Task An_effect_journaled_before_the_change_with_a_retry_policy_is_attempted_again_under_that_policy()
    {
        // Three attempts in all. Attempt 1 was cut off, so attempt 2 is due, and its date has passed.
        await WriteJournal("counter-old-policy", [Read("issued-probe-policy-3")]);

        var entity = Spawn("old-policy", ProbeEntity.Definition(_world));

        await AwaitAssertAsync(async () => Assert.Equal(1, (await StateOf(entity)).Count), cancellationToken: Ct);
        var (effect, delivery) = Assert.Single(_world.ProbeCalls);
        Assert.Equal(new Probe("a"), effect);
        Assert.Equal(new EffectDelivery(new EffectId("counter/old-policy/1", 1), 2), delivery);
        Assert.Equal(["answer:a"], _world.DecidedNow); // answered, not given up
    }

    [Fact]
    public async Task A_snapshot_written_before_the_change_with_a_pending_effect_gives_it_up_through_the_definitions_GaveUp()
    {
        var snapshot = Assert.IsType<EntitySnapshot>(Read("snapshot-pending-compute"));
        Assert.Equal(new CounterState(2, false, Request), snapshot.State);
        Assert.Equal(
            new PendingEffect(4, new Compute(Request), IssuedAt, null, 1, IssuedAt.AddSeconds(35)),
            Assert.Single(snapshot.Pending));

        // The pending effect was issued by entry 4, and the snapshot was taken there. The four entries below it are
        // not replayed. They are other events on purpose, so a recovery that did not use the snapshot would show.
        var below = Enumerable.Repeat(Event(new Incremented(10)), 4).ToArray();
        await WriteJournal("counter-old-snapshot", below, snapshot);

        var entity = Spawn("old-snapshot", Counter.Definition);
        var state = await StateOf(entity);

        Assert.Equal(new CounterState(2, false, Pending: null, LastGaveUp: "compute:Interrupted"), state);
        Assert.Empty(await PendingOf(entity));
        Assert.DoesNotContain("compute:3", Recorder.Entries);
    }
}

/// <summary>Writes the given entries, and a snapshot when asked, under the persistence id of an entity.</summary>
file sealed class OldJournal : ReceivePersistentActor
{
    public OldJournal(string persistenceId)
    {
        PersistenceId = persistenceId;
        IActorRef? asker = null;
        Recover<object>(_ => { });
        Command<Append>(append =>
        {
            var left = append.Entries.Length;
            var sender = Sender;
            PersistAll(append.Entries, _ =>
            {
                if (--left == 0) sender.Tell(new Done());
            });
        });
        Command<SnapshotOf>(snapshot =>
        {
            asker = Sender;
            SaveSnapshot(snapshot.State);
        });
        Command<SaveSnapshotSuccess>(_ => asker!.Tell(new Done()));
    }

    public override string PersistenceId { get; }

    public sealed record Append(object[] Entries);

    public sealed record SnapshotOf(object State);

    public sealed record Done;
}
