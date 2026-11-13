using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Bells;
using Akka.Durable.Tests.Hosting;
using Akka.Event;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// An <c>Execute</c> by an entity whose definition has no <c>GaveUp</c> is refused before the write: <c>Decide</c> could
/// never hear that the effect got no answer. The same bell with its <c>GaveUp</c> journals and runs the same effects:
/// see <see cref="GiveUpMappingSpecs"/>.
/// </summary>
public sealed class ExecuteWithoutGaveUpSpecs(ITestOutputHelper output) : BellActorSpec(output)
{
    private const string ArgumentToAdd =
        "Add the argument \"GaveUp: gaveUp => new YourCommand(gaveUp.Reason)\" to the EntityDefinition of [bell], " +
        "where YourCommand is one of its commands (BellCommand).";

    protected override EntityDefinition<BellState, BellCommand, BellEvent> Definition(EntityOptions? options) =>
        Bell.Definition(World, options) with { GaveUp = null };

    [Theory]
    [InlineData("whisper", nameof(Whisper))] // a handler with no answer: the host cannot see at start whose effect it is
    [InlineData("ring", nameof(Ring))] // a handler that answers with the entity's command type
    public async Task The_effect_and_the_events_of_its_decision_are_not_journaled_the_handler_does_not_run_and_the_error_names_the_argument_to_add(
        string kind, string effect)
    {
        var entity = Spawn("no-gave-up");
        Assert.Equal(0, (await SnapshotOf(entity)).SequenceNr);
        var log = LogCollector.Attach(Sys);

        Send(entity, new Sound(kind, "a")); // one event and one effect

        // The refusal goes to supervision with this message, and the entity logs an error of its own.
        var refusal =
            $"Entity type [bell] issued the effect {effect}, and its definition does not say which command Decide " +
            $"gets when an effect gets no answer. {ArgumentToAdd} The effect was not journaled and not run.";
        await AwaitAssertAsync(
            () => Assert.Contains(log, e => e is Error && e.Cause is InvalidOperationException && e.Cause.Message == refusal),
            Patience, cancellationToken: Ct);
        Assert.Contains(log, e => e is Error && e.Cause is null && e.Message.ToString() is { } text
            && text.Contains("bell/no-gave-up") && text.Contains(effect) && text.Contains(ArgumentToAdd));

        // The entity restarted and answers again. Nothing of the refused decision was written: not its event either.
        var after = await SnapshotOf(entity);
        Assert.Empty(after.State.Log);
        Assert.Equal(0, after.SequenceNr);
        Assert.Empty(await PendingOf(entity));
        Assert.Empty(await ReadRows("bell-no-gave-up"));
        Assert.Empty(World.Calls);

        // The next incarnation finds nothing to recover and nothing to give up.
        await Kill(entity);
        var next = await SnapshotOf(Spawn("no-gave-up"));
        Assert.Empty(next.State.Log);
        Assert.Equal(0, next.SequenceNr);
        Assert.Empty(World.Calls);
        Assert.Equal<object?>([new Sound(kind, "a")], World.Decided.Select(command => command.Value));
    }
}

/// <summary>
/// An effect whose handler answers with a type that is not the entity's command type is refused before the write: the
/// answer could never reach <c>Decide</c>. The host cannot see this at start, because such a handler looks like the
/// handler of another entity type or of a workflow.
/// </summary>
public sealed class ExecuteAnswerMismatchSpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly ProbeWorld _world = new();

    // Probe is the counter's effect, and its handler answers with text.
    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton(_world)
            .AddEffectHandler<Probe, string, RecordedExecuteStartupSpecs.TextAnswerHandler>();

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
        builder.WithLocalReminders(reminders => reminders
            .WithStorage(_ => new InMemoryReminderStorage())
            .WithResolver(system => new DirectEntityReminderResolver(system)));
    }

    private IActorRef Spawn(string id)
    {
        var entityId = new EntityId(id);
        var definition = ProbeEntity.Definition(_world);
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Sys.ActorOf(Props.Create(() =>
            new EntityActor<CounterState, CounterCommand, CounterEvent>(entityId, definition, services, time)));
    }

    private async Task<EntityStateReply> StateOf(IActorRef entity)
    {
        entity.Tell(GetEntityState.Instance, TestActor);
        return await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct);
    }

    [Fact]
    public async Task The_effect_and_the_events_of_its_decision_are_not_journaled_and_the_handler_does_not_run()
    {
        var entity = Spawn("mismatch");
        await StateOf(entity);
        var log = LogCollector.Attach(Sys);

        entity.Tell((CounterCommand)new NoHandler("issue:a"), TestActor); // one event and the effect Probe("a")

        const string refusal =
            "Entity type [counter] issued the effect Probe, and its handler TextAnswerHandler answers with a String. " +
            "The answer goes to the entity's Decide, so it must be a CounterCommand. Register the handler with " +
            "services.AddEffectHandler<Probe, CounterCommand, YourHandler>(), or with " +
            "services.AddEffectHandler<Probe, YourHandler>() for a handler with no answer. The effect was not " +
            "journaled and not run.";
        await AwaitAssertAsync(
            () => Assert.Contains(log, e => e is Error && e.Cause is InvalidOperationException && e.Cause.Message == refusal),
            cancellationToken: Ct);

        // The entity restarted and answers again. The event of the refused decision was not stored either.
        var after = await StateOf(entity);
        Assert.Equal(new CounterState(0, false), after.State);
        Assert.Equal(0, after.SequenceNr);
        entity.Tell(GetPendingEffects.Instance, TestActor);
        Assert.Empty((await ExpectMsgAsync<PendingEffectsReply>(cancellationToken: Ct)).Effects);
        Assert.Empty(_world.ProbeCalls);

        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
        var next = await StateOf(Spawn("mismatch"));
        Assert.Equal(new CounterState(0, false), next.State);
        Assert.Equal(0, next.SequenceNr);
        Assert.Empty(_world.ProbeCalls);
        Assert.Equal(["issue:a"], _world.DecidedNow); // nothing was given up: nothing was stored
    }
}
