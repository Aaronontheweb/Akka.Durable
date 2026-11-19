using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Persistence.TestKit;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// The entity actor with a counter whose effects have a short retry policy, on the Akka.Persistence TestKit journal.
/// A "crash" is a PoisonPill; the next incarnation recovers from the journal.
/// </summary>
public abstract class ObserverSpecsBase(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected readonly ProbeWorld World = new();

    // The entities this spec created, so a reminder is delivered to the entity it belongs to.
    private readonly ConcurrentDictionary<string, IActorRef> Spawned = new();

    protected abstract void AddObservers(IServiceCollection services);

    protected sealed override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<Observations>()
            .AddSingleton(World)
            .AddEffectHandler<Probe, CounterCommand, ProbeHandler>(Retry.Every(TimeSpan.FromMilliseconds(10), 3))
            .AddEffectHandler<Note, NoteHandler>();
        AddObservers(services);
    }

    // An Execute stores the wake reminder before its write, and a timer is a reminder.
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
        builder.WithLocalReminders(reminders => reminders
            .WithStorage(_ => new InMemoryReminderStorage())
            .WithResolver(_ => new SpawnedEntityResolver(Spawned)));
    }

    protected Observations Seen => Host.Services.GetRequiredService<Observations>();

    // The probe counter, with one more command: it sets a timer and cancels it in the same decision.
    private EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition()
    {
        var probe = ProbeEntity.Definition(World);
        return probe with
        {
            Decide = (state, command, ctx) => command is { Value: NoHandler { Note: "set-and-cancel" } }
                ? ctx.None().Schedule("never", TimeSpan.FromHours(1), new Noop()).CancelTimer("never")
                : probe.Decide(state, command, ctx),
        };
    }

    protected IActorRef Spawn(string id)
    {
        var entityId = new EntityId(id);
        var definition = Definition();
        var services = Host.Services;
        var time = Sys.Scheduler;
        var entity = Sys.ActorOf(Props.Create(() =>
            new EntityActor<CounterState, CounterCommand, CounterEvent>(entityId, definition, services, time)));
        Spawned[id] = entity;
        return entity;
    }

    protected void Send(IActorRef entity, CounterCommand command) => entity.Tell(command, TestActor);

    protected async Task<CounterState> StateOf(IActorRef entity)
    {
        entity.Tell(GetEntityState.Instance, TestActor);
        return (CounterState)(await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct)).State;
    }

    protected async Task Kill(IActorRef entity)
    {
        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
    }

    protected Task Await(Func<bool> done) =>
        AwaitAssertAsync(() => Assert.True(done()), cancellationToken: Ct);
}

/// <summary>Delivers a reminder to the entity actor that the spec spawned for its id.</summary>
internal sealed class SpawnedEntityResolver(ConcurrentDictionary<string, IActorRef> spawned) : Akka.Reminders.Sharding.IShardRegionResolver
{
    public IActorRef? TryResolve(ReminderEntity entity) => spawned.GetValueOrDefault(entity.EntityId);

    public void DeliverReminder(ReminderEntity entity, ReminderEnvelope envelope, IActorRef? sender = null) =>
        spawned[entity.EntityId].Tell(envelope, sender ?? ActorRefs.NoSender);
}

public sealed class DurableObserverSpecs(ITestOutputHelper output) : ObserverSpecsBase(output)
{
    // The failing observer is registered first: the one after it must still be called.
    protected override void AddObservers(IServiceCollection services) => services
        .AddDurableObserver<ThrowingObserver>()
        .AddDurableObserver<RecordingObserver>();

    [Fact]
    public async Task Events_are_observed_once_in_order_and_a_restart_does_not_report_them_again()
    {
        var entity = Spawn("events");
        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Send(entity, new Increment(2));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        var observed = Seen.Events;
        Assert.Equal([1L, 2L], observed.Select(e => e.SequenceNr));
        Assert.Equal([new Incremented(1), new Incremented(2)], observed.Select(e => e.Event));
        Assert.Equal([new Increment(1), new Increment(2)], observed.Select(e => e.Command));
        Assert.All(observed, e =>
        {
            Assert.Equal(Counter.TypeName, e.EntityType);
            Assert.Equal(new EntityId("events"), e.EntityId);
            Assert.Equal(new EventCause(CauseKind.Command, null), e.Cause);
        });

        // The new incarnation replays both events. It reports neither, and reports the next one as number 3.
        await Kill(entity);
        var recovered = Spawn("events");
        Assert.Equal(3, (await StateOf(recovered)).Count);
        Assert.Equal(2, Seen.Events.Length);

        Send(recovered, new Increment(4));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Assert.Equal([1L, 2L, 3L], Seen.Events.Select(e => e.SequenceNr));
    }

    [Fact]
    public async Task A_recorded_effect_is_observed_as_issued_started_and_acknowledged_and_its_answer_names_it()
    {
        Send(Spawn("acked"), new NoHandler("issue:a"));
        await Await(() => Seen.Effects.Length == 3 && Seen.Events.Length == 2);

        // The effect is issued by the entry after the first event, so its id ends in 2.
        const string id = "counter/acked/2";
        Assert.All(Seen.Effects, e => Assert.Equal(id, e.Id));
        Assert.Equal([EffectStage.Issued, EffectStage.AttemptStarted, EffectStage.Acknowledged],
            Seen.Effects.Select(e => e.Stage));
        Assert.Equal([1, 1, 1], Seen.Effects.Select(e => e.Attempt));
        Assert.Equal(new Probe("a"), Seen.Effects[0].Effect);

        // The handler's answer is a command, and the event it decided says which effect it came from.
        var answered = Seen.Events[1];
        Assert.Equal(new EventCause(CauseKind.EffectAnswer, id), answered.Cause);
        Assert.Equal(new NoHandler("answer:a"), answered.Command);
    }

    [Fact]
    public async Task A_failing_effect_is_observed_attempt_by_attempt_and_then_as_gave_up()
    {
        World.OnProbe = (_, _) => throw new InvalidOperationException("directory down");

        Send(Spawn("gaveup"), new NoHandler("issue:b"));
        await Await(() => Seen.Effects.Any(e => e.Stage == EffectStage.GaveUp) && Seen.Events.Length == 2);

        Assert.Equal(
            [EffectStage.Issued, EffectStage.AttemptStarted, EffectStage.AttemptStarted, EffectStage.AttemptStarted,
                EffectStage.GaveUp],
            Seen.Effects.Select(e => e.Stage));
        Assert.Equal([1, 1, 2, 3, 3], Seen.Effects.Select(e => e.Attempt));

        // The give-up command decided an event, and it names the effect that gave up.
        Assert.Equal(new EventCause(CauseKind.EffectAnswer, "counter/gaveup/2"), Seen.Events[1].Cause);
        Assert.IsType<GaveUpNoted>(Seen.Events[1].Event);
    }

    [Fact]
    public async Task A_timer_is_observed_as_set_and_fired_and_cancelled_and_its_event_names_it()
    {
        var entity = Spawn("timers");
        Send(entity, new NoHandler("later:t"));
        await Await(() => Seen.Timers.Length >= 2 && Seen.Events.Length >= 1);

        var set = Seen.Timers[0];
        Assert.Equal(("later", TimerStage.Set), (set.Key, set.Stage));
        Assert.Equal(set.At + TimeSpan.FromMilliseconds(100), set.Due);
        Assert.Equal(("later", TimerStage.Fired, (DateTimeOffset?)null), (Seen.Timers[1].Key, Seen.Timers[1].Stage, Seen.Timers[1].Due));

        // The timer delivered the command that decided the event. The Execute that follows stores the wake reminder.
        Assert.Equal(new EventCause(CauseKind.Timer, "later"), Seen.Events[0].Cause);
        Assert.Equal(new NoHandler("issue:t"), Seen.Events[0].Command);

        Send(entity, new NoHandler("set-and-cancel"));
        await Await(() => Seen.Timers.Length == 4);
        Assert.Equal([("never", TimerStage.Set), ("never", TimerStage.Cancelled)],
            Seen.Timers.Skip(2).Select(t => (t.Key, t.Stage)));

        // Only the entity's own timers were reported: the library's wake reminder was stored and removed meanwhile.
        await Await(() => Seen.Effects.Any(e => e.Stage == EffectStage.Acknowledged));
        Assert.DoesNotContain(Seen.Timers, t => t.Key.StartsWith("durable-", StringComparison.Ordinal));
    }

    [Fact]
    public async Task An_observer_that_throws_does_not_stop_the_entity_or_the_observers_after_it()
    {
        var entity = Spawn("throws");
        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Send(entity, new Increment(2));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Assert.Equal(3, (await StateOf(entity)).Count);
        Assert.Equal([1L, 2L], Seen.Events.Select(e => e.SequenceNr));
    }
}

public sealed class EventStreamPublisherSpecs(ITestOutputHelper output) : ObserverSpecsBase(output)
{
    protected override void AddObservers(IServiceCollection services) => services.AddDurableEventStreamPublisher();

    [Fact]
    public async Task The_publisher_sends_all_three_kinds_to_a_subscriber_of_the_marker_interface()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(IDurableObservation));

        Send(Spawn("published"), new NoHandler("later:p"));

        // A timer is set, fires, and its command writes an event and issues an effect: all in this order.
        await FishForMessageAsync(m => m is TimerObserved { Stage: TimerStage.Set }, cancellationToken: Ct);
        await FishForMessageAsync(m => m is TimerObserved { Stage: TimerStage.Fired }, cancellationToken: Ct);
        var saved = Assert.IsType<EventSaved>(await FishForMessageAsync(m => m is EventSaved, cancellationToken: Ct));
        Assert.Equal(new Incremented(100), saved.Event);
        await FishForMessageAsync(m => m is EffectObserved { Stage: EffectStage.Acknowledged }, cancellationToken: Ct);
    }
}

public sealed class WithoutEventStreamPublisherSpecs(ITestOutputHelper output) : ObserverSpecsBase(output)
{
    // No observer at all: nothing is published unless AddDurableEventStreamPublisher was called.
    protected override void AddObservers(IServiceCollection services)
    {
    }

    [Fact]
    public async Task Nothing_is_published_when_the_publisher_is_not_turned_on()
    {
        Sys.EventStream.Subscribe(TestActor, typeof(IDurableObservation));

        // Anything published would reach this actor before the reply does, so the first message must be the reply.
        Send(Spawn("silent"), new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
    }
}
