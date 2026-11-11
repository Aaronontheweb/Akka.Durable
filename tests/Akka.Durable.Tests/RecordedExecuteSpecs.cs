using Akka.Reminders.Storage;
using Akka.Reminders;
using Akka.Hosting;
using Akka.Actor;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Event;
using Akka.Hosting.TestKit;
using Akka.Persistence;
using Akka.Persistence.TestKit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// A recorded Execute against the entity actor itself, on the Akka.Persistence TestKit journal. A "crash" is a
/// PoisonPill, or a journal write that fails; the next incarnation recovers from what the journal kept.
/// </summary>
public sealed class RecordedExecuteSpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private static readonly TimeSpan NoMsgWindow = TimeSpan.FromMilliseconds(300);
    private static readonly TimeSpan ShortTimeout = TimeSpan.FromMilliseconds(300);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly ProbeWorld _world = new();

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddProbe(_world);

    // An Execute stores the wake reminder before its write.
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
        builder.WithLocalReminders(reminders => reminders
            .WithStorage(_ => new InMemoryReminderStorage())
            .WithResolver(system => new DirectEntityReminderResolver(system)));
    }

    #region Helpers

    private IActorRef Spawn(string id, EntityOptions? options = null)
    {
        var entityId = new EntityId(id);
        var definition = ProbeEntity.Definition(_world, options);
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Sys.ActorOf(Props.Create(() =>
            new EntityActor<CounterState, CounterCommand, CounterEvent>(entityId, definition, services, time)));
    }

    // Typed as the union so the case record is boxed as a CounterCommand.
    private void Send(IActorRef entity, CounterCommand command) => entity.Tell(command, TestActor);

    private void Say(IActorRef entity, string note) => Send(entity, new NoHandler(note));

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

    private Task AwaitState(IActorRef entity, Func<CounterState, bool> done) =>
        AwaitAssertAsync(async () => Assert.True(done(await StateOf(entity))), cancellationToken: Ct);

    private Task AwaitProbeCalls(int count) =>
        AwaitAssertAsync(() => Assert.Equal(count, _world.ProbeCalls.Count), cancellationToken: Ct);

    private async Task Kill(IActorRef entity)
    {
        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
    }

    /// <summary>Waits until the entity has stopped by itself, for example after a failed journal write.</summary>
    private async Task AwaitStopped(IActorRef entity)
    {
        Watch(entity);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
    }

    /// <summary>The state of a new incarnation, read once its recovery is done.</summary>
    private async Task<CounterState> Recover(string id, EntityOptions? options = null) => await StateOf(Spawn(id, options));

    private static EntityOptions WithTimeout() => EntityOptions.Default with { EffectTimeout = ShortTimeout };

    private static string[] GaveUpDecisions(IEnumerable<string> decided) =>
        [.. decided.Where(d => d.StartsWith("gaveup:", StringComparison.Ordinal))];

    private static object Event(CounterEvent @event) => @event;

    private int HandlerCalls => _world.ProbeCalls.Count + _world.NoteCalls.Count;

    private async Task WriteJournal(string persistenceId, params object[] entries)
    {
        var writer = Sys.ActorOf(Props.Create(() => new JournalScript(persistenceId)));
        writer.Tell(new JournalScript.Append(entries), TestActor);
        await ExpectMsgAsync<JournalScript.Done>(cancellationToken: Ct);
        await Kill(writer);
    }

    #endregion

    #region Crash before or after the attempt

    [Fact]
    public async Task An_effect_stored_but_never_started_gives_up_as_interrupted_once_and_is_not_run()
    {
        // The process stopped after the write and before the handler ran.
        await WriteJournal("counter-never-started",
            Event(new Incremented(1)), new EffectIssued(new Probe("a"), DateTimeOffset.UnixEpoch));

        var first = Spawn("never-started");
        var state = await StateOf(first);
        Assert.Equal("a:Interrupted:1:True", state.LastGaveUp);
        Assert.Equal(1, state.Count);

        await Kill(first);
        Assert.Equal("a:Interrupted:1:True", (await Recover("never-started")).LastGaveUp);

        // Decided once in all, and the handler never ran.
        Assert.Single(GaveUpDecisions(_world.Decided));
        Assert.Empty(_world.ProbeCalls);
    }

    [Fact]
    public async Task Commands_that_arrive_during_recovery_are_decided_after_the_give_up_of_the_interrupted_effect()
    {
        await WriteJournal("counter-ordering",
            Event(new Incremented(1)), new EffectIssued(new Probe("a"), DateTimeOffset.UnixEpoch));

        var entity = Spawn("ordering");
        Say(entity, "ping");
        Say(entity, "ping");
        await StateOf(entity);

        Assert.Equal(["gaveup:a:Interrupted:1:True", "ping", "ping"], _world.DecidedNow);
    }

    [Fact]
    public async Task Several_interrupted_effects_give_up_one_by_one_in_the_order_they_were_issued()
    {
        // Names in reverse alphabetical order, so that sorting by anything but issue order is seen.
        _world.Hold("b", "a");
        var entity = Spawn("several");
        Say(entity, "issue2:b,a");
        await AwaitProbeCalls(2);
        await Kill(entity);

        var state = await Recover("several");

        Assert.Equal(
            ["issue2:b,a", "gaveup:b:Interrupted:1:True", "gaveup:a:Interrupted:1:True"], _world.DecidedNow);
        Assert.Equal("a:Interrupted:1:True", state.LastGaveUp); // the last decision saw the state the first one left
        Assert.Equal(2, _world.ProbeCalls.Count);
    }

    [Fact]
    public async Task A_give_up_that_issues_an_Execute_runs_the_new_effect_once_and_never_the_old_one()
    {
        _world.OnGaveUp = (what, ctx) =>
        {
            if (!what.StartsWith("a:Interrupted", StringComparison.Ordinal)) return null;
            Decision<CounterCommand, CounterEvent> again = ctx.None().Execute(new Probe("again"));
            return again;
        };
        _world.Hold("a");
        var entity = Spawn("retry");
        Say(entity, "issue:a");
        await AwaitProbeCalls(1);
        await Kill(entity);

        var recovered = Spawn("retry");
        await AwaitProbeCalls(2);
        _world.Release("a"); // the old handler's token was cancelled with its incarnation; nothing may come of this
        await AwaitState(recovered, s => s.Count == 101); // 100 from the first decision, 1 from the answer of "again"

        Assert.Equal(["a", "again"], _world.ProbeCalls.Select(c => c.Effect.Name));
        var (first, second) = (_world.ProbeCalls.First().Delivery, _world.ProbeCalls.Last().Delivery);
        Assert.NotEqual(first.Id, second.Id);
        Assert.Equal(1, second.Attempt);
        Assert.Contains("answer:again", _world.Decided);
        Assert.DoesNotContain("answer:a", _world.Decided);
        Assert.Empty(await PendingOf(recovered));
    }

    [Fact]
    public async Task The_answer_of_an_interrupted_effect_is_dropped_when_it_arrives_late()
    {
        _world.Hold("a");
        var entity = Spawn("late");
        Say(entity, "issue:a");
        await AwaitProbeCalls(1);
        await Kill(entity);

        var recovered = Spawn("late");
        _world.Release("a");
        await ExpectNoMsgAsync(NoMsgWindow, Ct);

        var state = await StateOf(recovered);
        Assert.Equal(100, state.Count);
        Assert.Equal("a:Interrupted:1:True", state.LastGaveUp);
        Assert.DoesNotContain("answer:a", _world.Decided);
    }

    [Theory]
    [InlineData("issue:a", false)]
    [InlineData("note:a", true)]
    public async Task An_attempt_whose_acknowledgement_was_not_journaled_is_interrupted_and_not_run_again(
        string command, bool noAnswer)
    {
        var entity = Spawn("ack-lost");
        Watch(entity);

        // The handler runs and answers, but the write with its answer fails: the process is gone. The test journal
        // stores entry by entry, so the write is refused at its first entry: the answer's event, or the
        // acknowledgement alone.
        Func<object, bool> refuse = noAnswer
            ? p => p is EffectAcknowledged
            : p => p is CounterEvent e && e.Value is Incremented { By: 1 };
        await WithJournalWrite(write => write.SetInterceptorAsync(new FailWhen(refuse)), async () =>
        {
            Say(entity, command);
            await ExpectTerminatedAsync(entity, cancellationToken: Ct);
        });

        var state = await Recover("ack-lost");

        Assert.Equal("a:Interrupted:1:True", state.LastGaveUp);
        Assert.Equal(noAnswer ? 0 : 100, state.Count); // the answer's event was not stored either
        Assert.Equal(1, HandlerCalls);
        Assert.Single(GaveUpDecisions(_world.Decided));
    }

    #endregion

    #region One write: events and effect, or neither

    // The test journal stores entry by entry, so the refused entry is the first of its write. A store that is atomic
    // is checked against SQLite, in RecordedExecuteSqlMixedSpecs.
    [Theory]
    [InlineData("issue:a")] // the event is the first entry, the effect follows
    [InlineData("note:a")] // the effect is the only entry
    public async Task A_failed_write_runs_no_handler_and_leaves_nothing_stored_or_pending(string command)
    {
        var entity = Spawn("neither");
        await StateOf(entity);

        await WithJournalWrite(
            write => write.SetInterceptorAsync(new FailWhen(p => p is CounterEvent || p is EffectIssued)), async () =>
            {
                Say(entity, command);
                await AwaitStopped(entity);
            });

        var state = await Recover("neither");

        Assert.Equal(new CounterState(0, false), state);
        Assert.Equal(0, HandlerCalls);
        Assert.Equal([command], _world.DecidedNow); // nothing was given up: nothing was stored
    }

    [Fact]
    public async Task A_rejected_write_runs_no_handler_and_leaves_nothing_pending()
    {
        var entity = Spawn("rejected");
        await StateOf(entity);

        await WithJournalWrite(write => write.Reject(), async () =>
        {
            Say(entity, "issue:a");
            await ExpectNoMsgAsync(NoMsgWindow, Ct);
        });

        Assert.Empty(_world.ProbeCalls);
        var state = await StateOf(entity);
        Assert.Equal(0, state.Count);
        Assert.Empty(await PendingOf(entity));
    }

    [Fact]
    public async Task An_effect_is_stored_in_the_same_write_as_the_events_and_its_id_is_its_sequence_number()
    {
        _world.Hold("a");
        var entity = Spawn("same-write");
        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Say(entity, "issue:a"); // one event, one effect
        await AwaitProbeCalls(1);

        // Rows: the earlier event, then the decision's event and its effect, one after the other.
        var rows = await ReadRows("counter-same-write");
        Assert.Equal([1L, 2L, 3L], rows.Select(r => r.SequenceNr));
        Assert.Equal(Event(new Incremented(1)), rows[0].Payload);
        Assert.Equal(Event(new Incremented(100)), rows[1].Payload);
        var issued = Assert.IsType<EffectIssued>(rows[2].Payload);
        Assert.Equal(new Probe("a"), issued.Effect);

        // The id of the effect is the sequence number of its row.
        Assert.Equal(new EffectDelivery(new EffectId("counter/same-write/3", 3), 1), _world.ProbeCalls.Single().Delivery);
    }

    private async Task<(long SequenceNr, object Payload)[]> ReadRows(string persistenceId)
    {
        var probe = CreateTestProbe();
        Sys.ActorOf(Props.Create(() => new RowReader(persistenceId, probe.Ref)));
        return await probe.ExpectMsgAsync<(long, object)[]>(cancellationToken: Ct);
    }

    [Fact]
    public async Task An_effect_type_without_a_handler_is_refused_before_the_write_and_the_entity_restarts()
    {
        var entity = Spawn("unrouted");
        await StateOf(entity);

        await EventFilter.Exception<InvalidOperationException>(contains: "AddEffectHandler<Unrouted")
            .ExpectOneAsync(() =>
            {
                Say(entity, "unrouted"); // persists an event and issues the effect
                return Task.CompletedTask;
            }, Ct);

        // The event of the refused decision was not stored either, and the entity answers again.
        Assert.Equal(new CounterState(0, false), await StateOf(entity));
        Assert.Equal(new CounterState(0, false), await Recover("unrouted"));
        Assert.Empty(await PendingOf(entity));
    }

    #endregion

    #region The one attempt

    // The way a handler misbehaves, and the give-up it must become.
    [Theory]
    [InlineData("issue:a", "throw", "a:Failed:1:True")]
    [InlineData("issue:a", "hang", "a:TimedOut:1:True")]
    [InlineData("issue:a", "cancel", "a:Failed:1:True")] // cancelled by its own doing, not by the clock
    [InlineData("issue:a", "nothing", "a:Failed:1:True")] // an answer that is the empty union is no answer
    [InlineData("note:a", "throw", "a:Failed:1:True")]
    [InlineData("note:a", "hang", "a:TimedOut:1:True")]
    [InlineData("note:a", "cancel", "a:Failed:1:True")]
    public async Task A_handler_that_does_not_answer_gives_up_once_with_the_right_reason(
        string command, string how, string expectedGaveUp)
    {
        static async Task Misbehave(string how, CancellationToken ct)
        {
            switch (how)
            {
                case "throw": throw new InvalidOperationException("boom");
                case "cancel": throw new OperationCanceledException();
                default: await Task.Delay(Timeout.Infinite, ct); break;
            }
        }

        _world.OnProbe = async (_, ct) =>
        {
            if (how == "nothing") return default;
            await Misbehave(how, ct);
            throw new InvalidOperationException("unreachable");
        };
        _world.OnNote = (_, ct) => Misbehave(how, ct);
        var entity = Spawn("misbehave", WithTimeout());

        Say(entity, command);

        await AwaitState(entity, s => s.LastGaveUp == expectedGaveUp);
        Assert.Empty(await PendingOf(entity));
        Assert.Equal(1, HandlerCalls);
        await Kill(entity);
        await Recover("misbehave", WithTimeout());
        Assert.Single(GaveUpDecisions(_world.Decided)); // settled: the next incarnation has nothing to give up
        Assert.Equal(1, HandlerCalls);
    }

    [Fact]
    public async Task An_answer_that_comes_after_the_time_out_is_not_decided()
    {
        var late = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // A handler that does not look at its token: it answers when it wants to.
        _world.OnProbe = async (_, _) =>
        {
            await late.Task;
            return new NoHandler("answer:late");
        };
        var entity = Spawn("late-answer", WithTimeout());

        Say(entity, "issue:a");
        await AwaitState(entity, s => s.LastGaveUp == "a:TimedOut:1:True");
        late.SetResult();
        await ExpectNoMsgAsync(NoMsgWindow, Ct);

        var state = await StateOf(entity);
        Assert.Equal(100, state.Count);
        Assert.DoesNotContain("answer:late", _world.Decided);
    }

    [Fact]
    public async Task A_handler_with_no_answer_that_returns_is_acknowledged_and_Decide_gets_nothing_more()
    {
        var entity = Spawn("note");

        Say(entity, "note:a");
        await AwaitAssertAsync(() => Assert.Single(_world.NoteCalls), cancellationToken: Ct);
        await AwaitAssertAsync(async () => Assert.Empty(await PendingOf(entity)), cancellationToken: Ct);
        await Kill(entity);
        await Recover("note");

        Assert.Equal(["note:a"], _world.DecidedNow); // not given up, not interrupted after the restart
        Assert.Single(_world.NoteCalls);
    }

    [Fact]
    public async Task Two_effects_from_one_decision_each_run_once_and_each_is_answered_once()
    {
        var entity = Spawn("two");

        Say(entity, "issue2:a,b");

        await AwaitState(entity, s => s.Count == 2);
        Assert.Equal(["a", "b"], _world.ProbeCalls.Select(c => c.Effect.Name).Order());
        Assert.Equal(
            ["answer:a", "answer:b"],
            _world.DecidedNow.Where(d => d.StartsWith("answer:", StringComparison.Ordinal)).Order());
        Assert.Empty(await PendingOf(entity));
    }

    [Fact]
    public async Task Many_effects_from_one_decision_keep_their_issue_order_and_their_own_ids_through_a_crash()
    {
        const int count = 12; // e10 and e11 sort before e2 by name
        var names = Enumerable.Range(0, count).Select(i => $"e{i}").ToArray();
        _world.Hold(names);
        var entity = Spawn("many");
        Say(entity, $"many:{count}");
        await AwaitProbeCalls(count);

        Assert.Equal(names.Select(name => new Probe(name)), (await PendingOf(entity)).Cast<Probe>());
        Assert.Equal(count, _world.ProbeCalls.Select(c => c.Delivery.Id).Distinct().Count());
        await Kill(entity);

        await Recover("many");

        Assert.Equal(names.Select(name => $"gaveup:{name}:Interrupted:1:True"), GaveUpDecisions(_world.Decided));
    }

    [Fact]
    public async Task Two_equal_effects_from_one_decision_are_two_effects_with_two_ids()
    {
        _world.Hold("a");
        var entity = Spawn("twins");

        Say(entity, "issue2:a,a");
        await AwaitProbeCalls(2);

        Assert.Equal([new Probe("a"), new Probe("a")], await PendingOf(entity));
        Assert.Equal(2, _world.ProbeCalls.Select(c => c.Delivery.Id).Distinct().Count());

        _world.Release("a");
        await AwaitState(entity, s => s.Count == 2);
        Assert.Empty(await PendingOf(entity));
    }

    [Fact]
    public async Task The_decision_on_an_answer_can_issue_an_effect_and_the_answered_one_is_no_longer_pending()
    {
        _world.OnProbe = (effect, _) => Task.FromResult<CounterCommand>(
            effect.Name == "a" ? new NoHandler("issue:follow") : new NoHandler($"answer:{effect.Name}"));
        _world.Hold("follow");
        var entity = Spawn("chain");
        Say(entity, "issue:a");
        await AwaitProbeCalls(2);

        // The answer of "a" and the issue of "follow" were one write: only "follow" is pending.
        Assert.Equal([new Probe("follow")], await PendingOf(entity));

        _world.Release("follow");
        await AwaitState(entity, s => s.Count == 201);
        Assert.Equal(["a", "follow"], _world.ProbeCalls.Select(c => c.Effect.Name));
        Assert.Empty(await PendingOf(entity));
    }

    #endregion

    #region Decisions that do not persist

    [Theory]
    [InlineData("answer-ignored")]
    [InlineData("answer-unhandled")]
    [InlineData("give-up-ignored")]
    [InlineData("give-up-unhandled")]
    public async Task An_answer_or_give_up_that_Decide_ignores_still_settles_the_effect(string kind)
    {
        switch (kind)
        {
            case "answer-ignored":
                _world.OnProbe = (_, _) => Task.FromResult<CounterCommand>(new Noop());
                break;
            case "answer-unhandled":
                _world.OnProbe = (_, _) => Task.FromResult<CounterCommand>(new Unsupported());
                break;
            default:
                _world.OnProbe = (_, _) => throw new InvalidOperationException("boom");
                _world.OnGaveUp = (_, ctx) => kind == "give-up-ignored" ? ctx.Ignore() : ctx.Unhandled();
                break;
        }

        var entity = Spawn("settled");
        Say(entity, "issue:a");
        await AwaitAssertAsync(() => Assert.Equal(2, _world.Decided.Count), cancellationToken: Ct);
        await AwaitAssertAsync(async () => Assert.Empty(await PendingOf(entity)), cancellationToken: Ct);
        await Kill(entity);

        var state = await Recover("settled");

        // The next incarnation has nothing to give up: the effect was settled, so Decide sees no second command.
        Assert.Equal(new CounterState(100, false), state);
        Assert.Equal(2, _world.Decided.Count);
        Assert.Single(_world.ProbeCalls);
    }

    [Fact]
    public async Task An_answer_that_Decide_stashes_is_decided_once_when_the_stash_is_released()
    {
        _world.Hold("a");
        var entity = Spawn("stashed-answer");
        Send(entity, new LockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Say(entity, "issue:a");
        await AwaitProbeCalls(1);

        _world.Release("a"); // the answer arrives while the counter is locked
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        Assert.Equal(100, (await StateOf(entity)).Count);

        Send(entity, new UnlockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        await AwaitState(entity, s => s.Count == 101);
        await AwaitAssertAsync(async () => Assert.Empty(await PendingOf(entity)), cancellationToken: Ct);
        await Kill(entity);

        var state = await Recover("stashed-answer");
        Assert.Equal(101, state.Count);
        Assert.Null(state.LastGaveUp); // answered, so not given up as interrupted after the restart
        Assert.Single(_world.ProbeCalls);
    }

    [Fact]
    public async Task An_answer_stashed_at_a_crash_is_not_lost_as_acknowledged_the_effect_gives_up_as_interrupted()
    {
        _world.Hold("a");
        var entity = Spawn("stashed-crash");
        Send(entity, new LockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Say(entity, "issue:a");
        await AwaitProbeCalls(1);
        _world.Release("a");
        await ExpectNoMsgAsync(NoMsgWindow, Ct); // the answer is stashed: Decide has not decided on it
        await Kill(entity);

        var state = await Recover("stashed-crash");

        // Exactly one of answer and give-up: the answer was never decided, so the effect gives up.
        Assert.Equal("a:Interrupted:1:True", state.LastGaveUp);
        Assert.Equal(100, state.Count);
        Assert.Single(_world.ProbeCalls);
    }

    [Fact]
    public async Task A_give_up_that_Decide_stashes_after_a_restart_is_decided_once_when_the_stash_is_released()
    {
        _world.Hold("a");
        var entity = Spawn("stashed-give-up");
        Send(entity, new LockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Say(entity, "issue:a");
        await AwaitProbeCalls(1);
        await Kill(entity);

        var stashing = true;
        _world.OnGaveUp = (_, ctx) =>
        {
            if (!stashing) return null;
            Decision<CounterCommand, CounterEvent> stash = ctx.Stash();
            return stash;
        };
        var recovered = Spawn("stashed-give-up");
        Assert.Null((await StateOf(recovered)).LastGaveUp); // stashed: not decided yet

        stashing = false;
        Send(recovered, new UnlockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        await AwaitState(recovered, s => s.LastGaveUp == "a:Interrupted:1:True");
        await AwaitAssertAsync(async () => Assert.Empty(await PendingOf(recovered)), cancellationToken: Ct);
        await Kill(recovered);

        // Decided once for good: the next incarnation has nothing left to give up.
        var decidedBefore = GaveUpDecisions(_world.Decided).Length;
        Assert.Equal("a:Interrupted:1:True", (await Recover("stashed-give-up")).LastGaveUp);
        Assert.Equal(decidedBefore, GaveUpDecisions(_world.Decided).Length);
        Assert.Single(_world.ProbeCalls);
    }

    [Fact]
    public async Task An_answer_that_Decide_cannot_decide_is_not_lost_the_effect_gives_up_as_interrupted_after_the_restart()
    {
        _world.OnProbe = (_, _) => Task.FromResult<CounterCommand>(new NoHandler("explode"));
        var entity = Spawn("explodes");

        // Decide throws on the answer: the entity restarts. The answer was not journaled, so it is not acknowledged.
        await EventFilter.Exception<InvalidOperationException>(contains: "Decide cannot decide this.").ExpectOneAsync(() =>
        {
            Say(entity, "issue:a");
            return Task.CompletedTask;
        }, Ct);

        await AwaitState(entity, s => s.LastGaveUp == "a:Interrupted:1:True");
        Assert.Single(_world.ProbeCalls);
        Assert.Single(GaveUpDecisions(_world.Decided));
        Assert.Empty(await PendingOf(entity));
    }

    #endregion

    #region Snapshots

    [Theory]
    [InlineData(1, true)]
    [InlineData(1, false)]
    [InlineData(3, false)]
    [InlineData(3, true)]
    public async Task An_effect_pending_across_a_snapshot_is_interrupted_and_a_settled_one_is_not(
        int snapshotEvery, bool deleteEvents)
    {
        var options = EntityOptions.Default with
        {
            SnapshotEvery = snapshotEvery, KeepSnapshots = 1, DeleteEventsOnSnapshot = deleteEvents,
        };
        _world.Hold("b");
        var entity = Spawn("snap", options);
        Send(entity, new Increment(1));
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        Say(entity, "issue2:a,b"); // "a" is answered, "b" waits
        await AwaitState(entity, s => s.Count == 2);
        await Kill(entity);

        var state = await Recover("snap", options);

        Assert.Equal(2, state.Count);
        Assert.Equal("b:Interrupted:1:True", state.LastGaveUp);
        Assert.Equal(
            ["increment:1", "issue2:a,b", "answer:a", "gaveup:b:Interrupted:1:True"], _world.DecidedNow);
        // Both handlers start in issue order, but run at the same time, so the order of their calls is not fixed.
        Assert.Equal(["a", "b"], _world.ProbeCalls.Select(c => c.Effect.Name).Order());
    }

    [Fact]
    public async Task A_snapshot_taken_after_an_effect_settled_does_not_bring_it_back()
    {
        var options = EntityOptions.Default with { SnapshotEvery = 1, KeepSnapshots = 1, DeleteEventsOnSnapshot = true };
        var entity = Spawn("snap-settled", options);
        Say(entity, "issue:a");
        await AwaitState(entity, s => s.Count == 101);
        await AwaitAssertAsync(async () => Assert.Empty(await PendingOf(entity)), cancellationToken: Ct);
        await Kill(entity);

        var state = await Recover("snap-settled", options);

        Assert.Equal(new CounterState(101, false), state);
        Assert.Equal(["issue:a", "answer:a"], _world.DecidedNow);
    }

    [Fact]
    public async Task A_snapshot_that_is_a_bare_state_is_read_as_nothing_pending_and_the_next_snapshot_carries_effects()
    {
        // Written by code that did not record effects: the state alone, with events behind it and one after it.
        var script = Sys.ActorOf(Props.Create(() => new JournalScript("counter-bare")));
        script.Tell(new JournalScript.Append([Event(new Incremented(1)), Event(new Incremented(2))]), TestActor);
        await ExpectMsgAsync<JournalScript.Done>(cancellationToken: Ct);
        script.Tell(new JournalScript.SnapshotOf(new CounterState(50, false)), TestActor);
        await ExpectMsgAsync<JournalScript.Done>(cancellationToken: Ct);
        script.Tell(new JournalScript.Append([Event(new Incremented(1))]), TestActor);
        await ExpectMsgAsync<JournalScript.Done>(cancellationToken: Ct);
        await Kill(script);

        var options = EntityOptions.Default with { SnapshotEvery = 1, KeepSnapshots = 1, DeleteEventsOnSnapshot = true };
        var entity = Spawn("bare", options);
        Assert.Equal(new CounterState(51, false), await StateOf(entity)); // the snapshot, and the event after it
        Assert.Empty(await PendingOf(entity));
        Assert.Empty(_world.Decided); // nothing was given up

        // The entity goes on from there, and a pending effect now lives on in a snapshot, not in events.
        _world.Hold("a");
        Say(entity, "issue:a");
        await AwaitProbeCalls(1);
        await Kill(entity);
        var state = await Recover("bare", options);

        Assert.Equal(151, state.Count);
        Assert.Equal("a:Interrupted:1:True", state.LastGaveUp);
    }

    #endregion

    #region Journal helpers

    private sealed class FailWhen(Func<object, bool> predicate) : IJournalInterceptor
    {
        public Task InterceptAsync(IPersistentRepresentation message) =>
            predicate(message.Payload) ? throw new InvalidOperationException("the journal is down") : Task.CompletedTask;
    }

    /// <summary>Writes the given entries, and a snapshot when asked, under the persistence id of an entity.</summary>
    private sealed class JournalScript : ReceivePersistentActor
    {
        public JournalScript(string persistenceId)
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

    /// <summary>Replays a persistence id and reports each entry with its sequence number.</summary>
    private sealed class RowReader : ReceivePersistentActor
    {
        public RowReader(string persistenceId, IActorRef replyTo)
        {
            PersistenceId = persistenceId;
            var rows = new List<(long, object)>();
            Recover<RecoveryCompleted>(_ => replyTo.Tell(rows.ToArray()));
            Recover<object>(entry => rows.Add((LastSequenceNr, entry)));
        }

        public override string PersistenceId { get; }
    }

    #endregion
}

/// <summary>The decider side: what <c>WhenGaveUp</c> hands to Decide.</summary>
public sealed class WhenGaveUpSpecs
{
    private static DeciderScenario<CounterState, CounterCommand, CounterEvent> Scenario() =>
        DeciderScenario.For(Counter.Definition);

    [Theory]
    [InlineData(GaveUpReason.Failed, 1, "a:Failed:1:True")]
    [InlineData(GaveUpReason.TimedOut, 1, "a:TimedOut:1:True")]
    [InlineData(GaveUpReason.Interrupted, 1, "a:Interrupted:1:True")]
    [InlineData(GaveUpReason.Failed, 4, "a:Failed:4:True")]
    public void The_command_is_the_one_the_effect_makes_of_its_reason_and_attempts(
        GaveUpReason reason, int attempts, string expected)
    {
        var scenario = Scenario();

        var outcome = attempts == 1
            ? scenario.WhenGaveUp(new Probe("a"), reason)
            : scenario.WhenGaveUp(new Probe("a"), reason, attempts);

        outcome.ThenPersisted(new GaveUpNoted(expected, EndsServiceIncrement: false));
    }
}
