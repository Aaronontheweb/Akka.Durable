using Akka.Actor;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Bells;
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
/// The bell entity on the entity actor itself, on the Akka.Persistence TestKit journal. The bell's effects are plain
/// records, and its definition has one <c>GaveUp</c> for all of them.
/// </summary>
public abstract class BellActorSpec(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    protected static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected BellWorld World { get; } = new();

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddBell(World);

    // An Execute stores the wake reminder before its write.
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
        builder.WithLocalReminders(reminders => reminders
            .WithStorage(_ => new InMemoryReminderStorage())
            .WithResolver(system => new DirectEntityReminderResolver(system)));
    }

    /// <summary>The definition the entity runs with.</summary>
    protected virtual EntityDefinition<BellState, BellCommand, BellEvent> Definition(EntityOptions? options) =>
        Bell.Definition(World, options);

    protected IActorRef Spawn(string id, EntityOptions? options = null)
    {
        var entityId = new EntityId(id);
        var definition = Definition(options);
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Sys.ActorOf(Props.Create(() =>
            new EntityActor<BellState, BellCommand, BellEvent>(entityId, definition, services, time)));
    }

    // Typed as the union so the case record is boxed as a BellCommand.
    protected void Send(IActorRef entity, BellCommand command) => entity.Tell(command, TestActor);

    protected async Task<StateSnapshot<BellState>> SnapshotOf(IActorRef entity)
    {
        entity.Tell(GetEntityState.Instance, TestActor);
        var reply = await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct);
        return new StateSnapshot<BellState>((BellState)reply.State, reply.SequenceNr);
    }

    protected async Task<string[]> LogOf(IActorRef entity) => [.. (await SnapshotOf(entity)).State.Log];

    protected async Task<object[]> PendingOf(IActorRef entity)
    {
        entity.Tell(GetPendingEffects.Instance, TestActor);
        return [.. (await ExpectMsgAsync<PendingEffectsReply>(cancellationToken: Ct)).Effects];
    }

    protected Task AwaitLog(IActorRef entity, Func<string[], bool> done) =>
        AwaitAssertAsync(async () => Assert.True(done(await LogOf(entity))), Patience, cancellationToken: Ct);

    protected Task AwaitCalls(int count) =>
        AwaitAssertAsync(() => Assert.Equal(count, World.Calls.Count), Patience, cancellationToken: Ct);

    protected async Task Kill(IActorRef entity)
    {
        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
    }

    /// <summary>Every entry the journal holds for a persistence id, with its sequence number.</summary>
    protected async Task<(long SequenceNr, object Payload)[]> ReadRows(string persistenceId)
    {
        var probe = CreateTestProbe();
        Sys.ActorOf(Props.Create(() => new BellRowReader(persistenceId, probe.Ref)));
        return await probe.ExpectMsgAsync<(long, object)[]>(cancellationToken: Ct);
    }

    protected static Task Hang(CancellationToken ct) => Task.Delay(Timeout.Infinite, ct);
}

/// <summary>
/// The give-up of an entity's effect, for each <see cref="GaveUpReason"/>: the definition's <c>GaveUp</c> function is
/// given the effect as it was issued, the reason and the attempts, and <c>Decide</c> gets the command it returns.
/// </summary>
public sealed class GiveUpMappingSpecs(ITestOutputHelper output) : BellActorSpec(output)
{
    private static EntityOptions ShortTimeout => EntityOptions.Default with { EffectTimeout = TimeSpan.FromMilliseconds(300) };

    /// <summary>
    /// The function was called once, with the issued effect by value, and Decide received the command it returned and
    /// no other give-up.
    /// </summary>
    private WorkGaveUp AssertGaveUpOnce(object effect, GaveUpReason reason, int attempts)
    {
        var notice = Assert.Single(World.Notices);
        Assert.Equal(effect, notice.Work);
        Assert.Equal(reason, notice.Reason);
        Assert.Equal(attempts, notice.Attempts);

        var returned = Assert.Single(World.Mapped).Value;
        Assert.Equal(new EffectFailed(effect, reason, attempts), returned);
        var decided = Assert.Single(World.Decided, command => command.Value is EffectFailed).Value;
        Assert.Equal(returned, decided);
        return notice;
    }

    [Theory]
    [InlineData("throw", GaveUpReason.Failed)]
    [InlineData("nothing", GaveUpReason.Failed)] // an answer that is the empty union is no answer
    [InlineData("hang", GaveUpReason.TimedOut)] // past EntityOptions.EffectTimeout
    public async Task A_handler_that_fails_or_runs_out_of_time_gives_up_through_the_definitions_GaveUp(
        string how, GaveUpReason reason)
    {
        World.AnswerWithNothing = how == "nothing";
        World.OnCall = (_, _, ct) => how switch
        {
            "throw" => throw new InvalidOperationException("boom"),
            "hang" => Hang(ct),
            _ => Task.CompletedTask,
        };
        var entity = Spawn("one-attempt", ShortTimeout);

        Send(entity, new Sound("ring", "a"));

        await AwaitLog(entity, log => log.Length == 2);
        var notice = AssertGaveUpOnce(new Ring("a", Volume: 3), reason, attempts: 1);
        if (how == "throw") Assert.Equal("boom", notice.Error);
        Assert.Equal(["asked:a", $"ring-failed:a:3:{reason}:1"], await LogOf(entity));
        Assert.Empty(await PendingOf(entity));
        Assert.Single(World.Calls);
    }

    [Fact]
    public async Task An_effect_cut_off_by_a_restart_gives_up_as_interrupted_through_the_definitions_GaveUp()
    {
        World.OnCall = (_, _, ct) => Hang(ct);
        var entity = Spawn("interrupted");
        Send(entity, new Sound("ring", "a"));
        await AwaitCalls(1);
        await Kill(entity);

        // The new incarnation has only the journaled effect. It decides the give-up before it answers.
        var recovered = Spawn("interrupted");
        var log = await LogOf(recovered);

        AssertGaveUpOnce(new Ring("a", Volume: 3), GaveUpReason.Interrupted, attempts: 1);
        Assert.Equal(["asked:a", "ring-failed:a:3:Interrupted:1"], log);
        Assert.Empty(await PendingOf(recovered));
        Assert.Single(World.Calls); // not run again
    }

    [Fact]
    public async Task A_limited_retry_policy_that_runs_out_gives_up_as_retries_exhausted_through_the_definitions_GaveUp()
    {
        World.OnCall = (_, delivery, _) => throw new InvalidOperationException($"refused {delivery.Attempt}");
        var entity = Spawn("exhausted", ShortTimeout);

        Send(entity, new Sound("flash", "b"));

        await AwaitLog(entity, log => log.Length == 2);
        var notice = AssertGaveUpOnce(new Flash("b"), GaveUpReason.RetriesExhausted, attempts: 3);
        Assert.Equal("refused 3", notice.Error); // the last failure
        Assert.Equal([1, 2, 3], World.Calls.Select(call => call.Delivery.Attempt));
        Assert.All(World.Calls, call => Assert.Equal(new Flash("b"), call.Effect));

        // Flash is not the effect Decide has an arm for: it took the arm for every other effect.
        Assert.Equal(["asked:b", "effect-failed:Flash:RetriesExhausted:3"], await LogOf(entity));
        Assert.Empty(await PendingOf(entity));
    }

    [Fact]
    public async Task One_GaveUp_serves_every_effect_and_Decide_tells_them_apart_by_the_effect_the_command_carries()
    {
        World.OnCall = (_, _, _) => throw new InvalidOperationException("boom");
        var entity = Spawn("two-effects");

        Send(entity, new Sound("ring", "a")); // the effect Decide has an arm for
        Send(entity, new Sound("whisper", "w")); // a handler with no answer: the arm for every other effect

        await AwaitLog(entity, log => log.Length == 4);
        var log = await LogOf(entity);
        Assert.Contains("ring-failed:a:3:Failed:1", log);
        Assert.Contains("effect-failed:Whisper:Failed:1", log);

        // The handlers run at the same time, so the order of the two give-ups is not fixed.
        Assert.Equal(2, World.Notices.Count);
        Assert.Contains(World.Notices, notice => Equals(notice.Work, new Ring("a", Volume: 3)) && notice.Reason == GaveUpReason.Failed);
        Assert.Contains(World.Notices, notice => Equals(notice.Work, new Whisper("w")) && notice.Reason == GaveUpReason.Failed);
        Assert.Equal(
            World.Mapped.Select(command => command.Value),
            World.Decided.Select(command => command.Value).Where(command => command is EffectFailed));
        Assert.Empty(await PendingOf(entity));
    }
}

/// <summary>The same two arms of the bell's Decide, through <c>DeciderScenario.WhenGaveUp</c>. No ActorSystem.</summary>
public sealed class GiveUpByEffectDeciderSpecs
{
    [Fact]
    public void WhenGaveUp_hands_Decide_the_command_the_definitions_GaveUp_makes_of_the_effect()
    {
        var world = new BellWorld();
        var scenario = DeciderScenario.For(Bell.Definition(world)).Given(new Heard("asked:a"));

        scenario.WhenGaveUp(new Ring("a", Volume: 9), GaveUpReason.TimedOut)
            .ThenPersisted(new Heard("ring-failed:a:9:TimedOut:1"))
            .ThenState(state => Assert.Equal(["asked:a", "ring-failed:a:9:TimedOut:1"], state.Log));

        scenario.WhenGaveUp(new Whisper("w"), GaveUpReason.Interrupted)
            .ThenPersisted(new Heard("effect-failed:Whisper:Interrupted:1"));

        scenario.WhenGaveUp(new Flash("b"), GaveUpReason.RetriesExhausted, attempts: 3)
            .ThenPersisted(new Heard("effect-failed:Flash:RetriesExhausted:3"));

        // The function was given each effect as the scenario passed it.
        Assert.Equal<object>([new Ring("a", Volume: 9), new Whisper("w"), new Flash("b")], world.Notices.Select(notice => notice.Work));
        Assert.Equal([1, 1, 3], world.Notices.Select(notice => notice.Attempts));
    }
}

/// <summary>Replays a persistence id and reports each entry with its sequence number.</summary>
internal sealed class BellRowReader : ReceivePersistentActor
{
    public BellRowReader(string persistenceId, IActorRef replyTo)
    {
        PersistenceId = persistenceId;
        var rows = new List<(long, object)>();
        Recover<RecoveryCompleted>(_ => replyTo.Tell(rows.ToArray()));
        Recover<object>(entry => rows.Add((LastSequenceNr, entry)));
    }

    public override string PersistenceId { get; }
}
