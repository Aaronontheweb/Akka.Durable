using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Hosting;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Persistence;
using Akka.Persistence.TestKit;
using Akka.Reminders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>
/// Retries of a recorded Execute against the entity actor itself, on the Akka.Persistence TestKit journal. A crash is a
/// PoisonPill; the next incarnation recovers from what the journal kept. The reminders of the entity go to dead
/// letters here, so only the entity's own timers and its recovery bring an attempt back.
/// </summary>
/// <remarks>
/// Policies: <see cref="Probe"/> is Backoff(100 ms, 300 ms, 4 attempts), <see cref="Note"/> is UntilAcknowledged(100 ms,
/// 200 ms), <see cref="Deliver"/> is Every(100 ms, 3 attempts). The effect time-out is 300 ms.
/// </remarks>
public sealed class OutboxSpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private static readonly TimeSpan NoMsgWindow = TimeSpan.FromMilliseconds(400);
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);
    private static readonly RetryPolicy ProbePolicy = Retry.Backoff(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(300), 4);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly ProbeWorld _world = new();
    private readonly OutboxWorld _mail = new();
    private readonly SwitchableReminderStorage _reminders = new();

    protected override void ConfigureServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton(_world)
            .AddSingleton(_mail)
            .AddEffectHandler<Probe, CounterCommand, ProbeHandler>(ProbePolicy)
            .AddEffectHandler<Note, NoteHandler>(Retry.UntilAcknowledged(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200)))
            .AddEffectHandler<Deliver, OutboxCommand, DeliverHandler>(Retry.Every(TimeSpan.FromMilliseconds(100), attempts: 3));

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);
        builder.WithLocalReminders(reminders => reminders
            .WithStorage(_ => _reminders)
            .WithResolver(system => new DirectEntityReminderResolver(system)));
    }

    #region Helpers

    private static EntityOptions Options => EntityOptions.Default with { EffectTimeout = TimeSpan.FromMilliseconds(300) };

    private IActorRef Spawn(string id)
    {
        var entityId = new EntityId(id);
        var definition = ProbeEntity.Definition(_world, Options);
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Sys.ActorOf(Props.Create(() =>
            new EntityActor<CounterState, CounterCommand, CounterEvent>(entityId, definition, services, time)));
    }

    private IActorRef SpawnOutbox(string id)
    {
        var entityId = new EntityId(id);
        var definition = Outbox.Definition with { Options = Options };
        var services = Host.Services;
        var time = Sys.Scheduler;
        return Sys.ActorOf(Props.Create(() =>
            new EntityActor<OutboxState, OutboxCommand, OutboxEvent>(entityId, definition, services, time)));
    }

    private void Say(IActorRef entity, string note) => entity.Tell((CounterCommand)new NoHandler(note), TestActor);

    private void Send(IActorRef entity, CounterCommand command) => entity.Tell(command, TestActor);

    private async Task<CounterState> StateOf(IActorRef entity)
    {
        entity.Tell(GetEntityState.Instance, TestActor);
        return (CounterState)(await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct)).State;
    }

    private async Task<OutboxState> MailStateOf(IActorRef entity)
    {
        entity.Tell(GetEntityState.Instance, TestActor);
        return (OutboxState)(await ExpectMsgAsync<EntityStateReply>(cancellationToken: Ct)).State;
    }

    private async Task<object[]> PendingOf(IActorRef entity)
    {
        entity.Tell(GetPendingEffects.Instance, TestActor);
        return [.. (await ExpectMsgAsync<PendingEffectsReply>(cancellationToken: Ct)).Effects];
    }

    private Task Eventually(Func<Task> check) => AwaitAssertAsync(check, Patience, cancellationToken: Ct);

    private Task AwaitState(IActorRef entity, Func<CounterState, bool> done) =>
        Eventually(async () => Assert.True(done(await StateOf(entity))));

    private Task AwaitProbeCalls(int count) => Eventually(() =>
    {
        Assert.Equal(count, _world.ProbeCalls.Count);
        return Task.CompletedTask;
    });

    private async Task Kill(IActorRef entity)
    {
        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
    }

    private async Task AwaitStopped(IActorRef entity)
    {
        Watch(entity);
        await ExpectTerminatedAsync(entity, cancellationToken: Ct);
    }

    private async Task<CounterState> Recover(string id) => await StateOf(Spawn(id));

    private static Task<CounterCommand> Answer(string name) => Task.FromResult<CounterCommand>(new NoHandler($"answer:{name}"));

    private static Task<CounterCommand> Hang(CancellationToken ct) => Task.Delay(Timeout.Infinite, ct).ContinueWith<CounterCommand>(_ => default);

    private int[] Attempts => [.. _world.ProbeCalls.Select(c => c.Delivery.Attempt)];

    private string[] GaveUpDecisions => [.. _world.Decided.Where(d => d.StartsWith("gaveup:", StringComparison.Ordinal))];

    private async Task<IReadOnlyList<ScheduledReminder>> RemindersOf(string id)
    {
        var listed = await Sys.ReminderClient().CreateClient(Counter.TypeName.Value, id).ListRemindersAsync(Ct);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    /// <summary>Writes entries under the persistence id of an entity and returns the sequence number of each.</summary>
    private async Task<long[]> WriteJournal(string persistenceId, params object[] entries)
    {
        var writer = Sys.ActorOf(Props.Create(() => new JournalScript(persistenceId)));
        writer.Tell(new JournalScript.Append(entries), TestActor);
        var done = await ExpectMsgAsync<JournalScript.Done>(cancellationToken: Ct);
        await Kill(writer);
        return done.Sequences;
    }

    #endregion

    #region Redelivery

    [Theory]
    [InlineData(0)] // an answer is never retried
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)] // the last attempt the policy allows
    public async Task A_handler_that_fails_n_times_is_attempted_n_plus_1_times_with_one_id_and_one_answer_is_decided(int failures)
    {
        _world.OnProbe = (_, _) => _world.ProbeCalls.Count <= failures ? throw new InvalidOperationException("not yet") : Answer("a");
        var entity = Spawn("fails-n");

        Say(entity, "issue:a");

        await AwaitState(entity, s => s.Count == 101);
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        Assert.Equal(Enumerable.Range(1, failures + 1), Attempts);
        Assert.Single(_world.ProbeCalls.Select(c => c.Delivery.Id).Distinct());
        Assert.Equal(new CounterState(101, false), await StateOf(entity));
        Assert.Single(_world.Decided, d => d == "answer:a");
        Assert.Empty(GaveUpDecisions);
        Assert.Empty(await PendingOf(entity));
    }

    [Theory]
    [InlineData("throw")]
    [InlineData("hang")]
    [InlineData("empty")]
    public async Task A_failed_first_attempt_is_attempted_again_whether_it_threw_ran_out_of_time_or_answered_with_nothing(string how)
    {
        _world.OnProbe = (_, ct) => _world.ProbeCalls.Count > 1 ? Answer("a") : how switch
        {
            "throw" => throw new InvalidOperationException("boom"),
            "hang" => Hang(ct),
            _ => Task.FromResult<CounterCommand>(default),
        };
        var entity = Spawn("kinds");

        Say(entity, "issue:a");

        await AwaitState(entity, s => s.Count == 101);
        Assert.Equal([1, 2], Attempts);
        Assert.Empty(GaveUpDecisions);
    }

    [Fact]
    public async Task The_wait_between_attempts_follows_the_backoff_of_the_policy()
    {
        var starts = new ConcurrentQueue<DateTimeOffset>();
        _world.OnProbe = (_, _) =>
        {
            starts.Enqueue(DateTimeOffset.UtcNow);
            return starts.Count <= 3 ? throw new InvalidOperationException("not yet") : Answer("a");
        };
        var entity = Spawn("backoff");

        Say(entity, "issue:a");

        await AwaitState(entity, s => s.Count == 101);
        var at = starts.ToArray();
        Assert.Equal(4, at.Length);
        // Waits are 100, 200 and then 300 ms (400 ms capped). The timer may fire a few ms early.
        TimeSpan[] atLeast = [TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(160), TimeSpan.FromMilliseconds(260)];
        for (var i = 0; i < 3; i++)
            Assert.True(at[i + 1] - at[i] >= atLeast[i], $"Attempt {i + 2} came {at[i + 1] - at[i]} after attempt {i + 1}.");
    }

    [Fact]
    public async Task A_handler_with_no_answer_that_fails_twice_is_acknowledged_once_and_is_not_run_again_after_a_restart()
    {
        _world.OnNote = (_, _) => _world.NoteCalls.Count <= 2 ? throw new InvalidOperationException("not yet") : Task.CompletedTask;
        var entity = Spawn("note-retried");

        Say(entity, "note:b");

        await Eventually(async () => Assert.Empty(await PendingOf(entity)));
        Assert.Equal([1, 2, 3], _world.NoteCalls.Select(c => c.Delivery.Attempt));
        Assert.Single(_world.NoteCalls.Select(c => c.Delivery.Id).Distinct());
        await Kill(entity);

        var recovered = Spawn("note-retried");
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        Assert.Empty(await PendingOf(recovered));
        Assert.Equal(3, _world.NoteCalls.Count);
        Assert.Empty(GaveUpDecisions);
    }

    [Fact]
    public async Task An_answer_of_an_earlier_attempt_that_arrives_after_the_next_attempt_was_answered_is_dropped()
    {
        // Attempt 1 ignores its token and answers when the spec lets it, long after it ran out of time.
        var late = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _world.OnProbe = async (_, _) =>
        {
            if (_world.ProbeCalls.Count > 1) return new NoHandler("answer:fresh");
            await late.Task;
            return new NoHandler("answer:late");
        };
        var entity = Spawn("late-retry");

        Say(entity, "issue:a");
        await AwaitState(entity, s => s.Count == 101);
        late.SetResult();
        await ExpectNoMsgAsync(NoMsgWindow, Ct);

        Assert.Equal(101, (await StateOf(entity)).Count);
        Assert.Contains("answer:fresh", _world.Decided);
        Assert.DoesNotContain("answer:late", _world.Decided);
        Assert.Empty(GaveUpDecisions);
    }

    #endregion

    #region Running out

    [Fact]
    public async Task A_policy_that_runs_out_gives_up_once_as_retries_exhausted_with_the_attempts_and_the_last_error()
    {
        // Attempt 1 runs out of time, attempts 2 and 3 throw. The error is the last failure, not the first.
        _mail.OnAttempt = (delivery, ct) => delivery.Attempt == 1
            ? Task.Delay(Timeout.Infinite, ct)
            : throw new InvalidOperationException($"refused {delivery.Attempt}");
        var entity = SpawnOutbox("exhausted");

        entity.Tell((OutboxCommand)new SendMail("m"), TestActor);

        await Eventually(async () => Assert.Single((await MailStateOf(entity)).Log));
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        Assert.Equal(["gaveup:m:RetriesExhausted:3:refused 3"], (await MailStateOf(entity)).Log);
        Assert.Equal([1, 2, 3], _mail.Deliveries.Select(d => d.Attempt));
        Assert.Empty(await PendingOf(entity));

        // Nothing is run or decided again after a restart.
        await Kill(entity);
        var recovered = SpawnOutbox("exhausted");
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        Assert.Equal(["gaveup:m:RetriesExhausted:3:refused 3"], (await MailStateOf(recovered)).Log);
        Assert.Equal(3, _mail.Deliveries.Count);
    }

    [Fact]
    public async Task Every_restart_that_cuts_an_attempt_off_uses_up_one_attempt_until_the_policy_is_exhausted()
    {
        _world.OnProbe = (_, ct) => Hang(ct);
        var entity = Spawn("restarts");
        Say(entity, "issue:a");
        await AwaitProbeCalls(1);

        // Attempt k is cut off by the kill; the next incarnation starts attempt k+1.
        for (var attempt = 2; attempt <= 4; attempt++)
        {
            await Kill(entity);
            entity = Spawn("restarts");
            await AwaitProbeCalls(attempt);
        }

        await Kill(entity);
        var state = await Recover("restarts"); // attempt 4 was the last the policy allows

        Assert.Equal("a:RetriesExhausted:4:True", state.LastGaveUp);
        Assert.Equal([1, 2, 3, 4], Attempts);
        Assert.Single(_world.ProbeCalls.Select(c => c.Delivery.Id).Distinct());
        Assert.Single(GaveUpDecisions);
    }

    [Fact]
    public async Task Attempts_of_a_pending_effect_go_on_after_a_crash_between_two_attempts_with_the_same_id()
    {
        // Attempt 1 fails, attempt 2 is in flight when the process stops.
        _world.OnProbe = (_, ct) => _world.ProbeCalls.Count switch
        {
            1 => throw new InvalidOperationException("not yet"),
            2 => Hang(ct),
            _ => Answer("a"),
        };
        var entity = Spawn("between");
        Say(entity, "issue:a");
        await AwaitProbeCalls(2);
        await Kill(entity);

        var recovered = Spawn("between");

        await AwaitState(recovered, s => s.Count == 101);
        Assert.Equal([1, 2, 3], Attempts); // the cut-off attempt counts
        Assert.Single(_world.ProbeCalls.Select(c => c.Delivery.Id).Distinct());
        Assert.Single(_world.Decided, d => d == "answer:a");
        Assert.Empty(GaveUpDecisions);
    }

    [Fact]
    public async Task An_effect_stored_but_never_started_is_delivered_after_recovery_with_the_id_of_its_record()
    {
        // The process stopped after the write and before the first attempt.
        var rows = await WriteJournal("counter-never-started",
            (CounterEvent)new Incremented(1),
            new EffectIssued(new Probe("a"), DateTimeOffset.UnixEpoch, StoredPolicy.Of(ProbePolicy), DateTimeOffset.UnixEpoch));
        var entity = Spawn("never-started");

        await AwaitState(entity, s => s.Count == 2); // 1 from the journal, 1 from the answer

        var (effect, delivery) = Assert.Single(_world.ProbeCalls);
        Assert.Equal(new Probe("a"), effect);
        Assert.Equal(new EffectId($"counter/never-started/{rows[1]}", rows[1]), delivery.Id);
        Assert.Equal(2, delivery.Attempt); // the attempt that may have started counts
        Assert.Empty(GaveUpDecisions);
        Assert.Empty(await PendingOf(entity));
    }

    [Fact]
    public async Task A_new_attempt_is_journaled_before_it_runs()
    {
        _world.OnProbe = (_, _) => _world.ProbeCalls.Count == 1 ? throw new InvalidOperationException("not yet") : Answer("a");
        var entity = Spawn("refused-retry");
        await StateOf(entity);

        await WithJournalWrite(write => write.SetInterceptorAsync(new FailWhen(p => p is EffectRetried)), async () =>
        {
            Say(entity, "issue:a");
            await AwaitStopped(entity);
        });

        // The refused record means attempt 2 never started, so no handler ran for it.
        Assert.Single(_world.ProbeCalls);
        var recovered = Spawn("refused-retry");
        await AwaitState(recovered, s => s.Count == 101);
        Assert.Equal([1, 2], Attempts); // the count did not skip a number either
    }

    #endregion

    #region The policy in the journal

    [Fact]
    public async Task A_pending_effect_keeps_the_policy_it_was_issued_with_when_the_registration_has_a_shorter_one()
    {
        // Issued without a limit; attempt 5 started. The registration today allows 4 attempts, which has run out.
        var rows = await WriteJournal("counter-keeps-policy",
            new EffectIssued(new Probe("a"), DateTimeOffset.UnixEpoch,
                StoredPolicy.Of(Retry.UntilAcknowledged(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200))), DateTimeOffset.UnixEpoch));
        await WriteJournal("counter-keeps-policy", new EffectRetried(rows[0], 5, DateTimeOffset.UnixEpoch));
        var entity = Spawn("keeps-policy");

        await AwaitState(entity, s => s.Count == 1);

        Assert.Equal([6], Attempts);
        Assert.Empty(GaveUpDecisions);
    }

    [Fact]
    public async Task A_pending_effect_that_was_issued_without_a_policy_is_interrupted_even_if_the_registration_has_one_now()
    {
        await WriteJournal("counter-no-policy",
            new EffectIssued(new Probe("a"), DateTimeOffset.UnixEpoch, Policy: null, NextAt: DateTimeOffset.UnixEpoch));
        var entity = Spawn("no-policy");

        var state = await StateOf(entity);

        Assert.Equal("a:Interrupted:1:True", state.LastGaveUp);
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        Assert.Empty(_world.ProbeCalls);
    }

    #endregion

    #region Decide stashes

    [Fact]
    public async Task An_answer_of_a_retried_effect_that_Decide_stashes_is_decided_once_after_the_stash_is_released_and_no_attempt_runs_meanwhile()
    {
        _world.OnProbe = (_, _) => _world.ProbeCalls.Count == 1 ? throw new InvalidOperationException("not yet") : Answer("a");
        var entity = Spawn("stashed-retry");
        Send(entity, new LockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Say(entity, "issue:a");
        await AwaitProbeCalls(2);
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        Assert.Equal(100, (await StateOf(entity)).Count);
        Assert.Equal(2, _world.ProbeCalls.Count);

        Send(entity, new UnlockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        await AwaitState(entity, s => s.Count == 101);
        await Kill(entity);

        var state = await Recover("stashed-retry");
        Assert.Equal(new CounterState(101, false), state);
        Assert.Equal(2, _world.ProbeCalls.Count); // settled: not attempted again
    }

    [Fact]
    public async Task A_give_up_for_an_exhausted_policy_that_Decide_stashes_is_decided_once_after_the_stash_is_released()
    {
        _world.OnProbe = (_, _) => throw new InvalidOperationException("never");
        var stashing = true;
        _world.OnGaveUp = (_, ctx) =>
        {
            if (!stashing) return null;
            Decision<CounterCommand, CounterEvent> stash = ctx.Stash();
            return stash;
        };
        var entity = Spawn("stashed-exhausted");
        Send(entity, new LockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);

        Say(entity, "issue:a");
        await AwaitProbeCalls(4);
        await ExpectNoMsgAsync(NoMsgWindow, Ct);
        Assert.Null((await StateOf(entity)).LastGaveUp);
        Assert.Equal(4, _world.ProbeCalls.Count);

        stashing = false;
        Send(entity, new UnlockCounter());
        await ExpectMsgAsync<Accepted>(cancellationToken: Ct);
        await AwaitState(entity, s => s.LastGaveUp == "a:RetriesExhausted:4:True");
        await Eventually(async () => Assert.Empty(await PendingOf(entity)));
        await Kill(entity);

        var decided = GaveUpDecisions.Length;
        Assert.Equal("a:RetriesExhausted:4:True", (await Recover("stashed-exhausted")).LastGaveUp);
        Assert.Equal(decided, GaveUpDecisions.Length);
        Assert.Equal(4, _world.ProbeCalls.Count);
    }

    #endregion

    #region Two policies in one entity

    [Fact]
    public async Task Two_effects_with_different_policies_each_follow_their_own()
    {
        // The probe has 4 attempts and always fails. The note has no limit and fails 5 times: more than the probe allows.
        _world.OnProbe = (_, _) => throw new InvalidOperationException("never");
        _world.OnNote = (_, _) => _world.NoteCalls.Count <= 5 ? throw new InvalidOperationException("not yet") : Task.CompletedTask;
        var entity = Spawn("two-policies");

        Say(entity, "issue:a");
        Say(entity, "note:b");

        await AwaitState(entity, s => s.LastGaveUp == "a:RetriesExhausted:4:True");
        await Eventually(async () => Assert.Empty(await PendingOf(entity)));
        Assert.Equal(4, _world.ProbeCalls.Count);
        Assert.Equal([1, 2, 3, 4, 5, 6], _world.NoteCalls.Select(c => c.Delivery.Attempt));
        Assert.Single(GaveUpDecisions); // only the probe gave up
        Assert.NotEqual(_world.ProbeCalls.First().Delivery.Id, _world.NoteCalls.First().Delivery.Id);
    }

    #endregion

    #region The wake reminder

    [Fact]
    public async Task The_wake_is_stored_before_the_write_that_leaves_an_effect_pending_and_only_once_for_two_effects()
    {
        var order = new ConcurrentQueue<string>();
        _reminders.Writing = reminder => order.Enqueue($"reminder:{reminder.Key.Name}");
        _world.Hold("a", "b");
        var entity = Spawn("wake-first");
        await StateOf(entity);

        await WithJournalWrite(write => write.SetInterceptorAsync(new RecordWhen(p => p is EffectIssued, () => order.Enqueue("journal"))), async () =>
        {
            Say(entity, "issue:a");
            await AwaitProbeCalls(1);
            Say(entity, "issue:b");
            await AwaitProbeCalls(2);
        });

        Assert.Equal(["reminder:durable-effects-wake", "journal", "journal"], order);
        var reminder = Assert.Single(await RemindersOf("wake-first"));
        Assert.Equal(new ReminderKey("durable-effects-wake"), reminder.Key);
    }

    [Fact]
    public async Task The_wake_is_removed_when_the_last_pending_effect_is_settled_and_is_there_after_a_recovery_while_one_is_pending()
    {
        _world.Hold("a");
        var entity = Spawn("wake-removed");
        Say(entity, "issue:a");
        await AwaitProbeCalls(1);
        Assert.Single(await RemindersOf("wake-removed"));
        await Kill(entity);

        var recovered = Spawn("wake-removed"); // attempt 2 is pending again
        await AwaitProbeCalls(2);
        Assert.Single(await RemindersOf("wake-removed"));

        _world.Release("a");
        await AwaitState(recovered, s => s.Count == 101);

        await Eventually(async () => Assert.Empty(await RemindersOf("wake-removed")));
    }

    #endregion
}

/// <summary>The timer keys the library keeps for itself, on every way a decision can ask for a timer.</summary>
public sealed class ReservedTimerKeySpecs
{
    private static Handled<CounterCommand, CounterEvent> None() => Decisions.None<CounterCommand, CounterEvent>();

    public static TheoryData<string, Func<string, Handled<CounterCommand, CounterEvent>>> Asks => new()
    {
        { "Schedule", key => None().Schedule(key, TimeSpan.FromSeconds(1), new Increment(1)) },
        { "Schedule recurring", key => None().Schedule(key, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), new Increment(1)) },
        { "CancelTimer", key => None().CancelTimer(key) },
        { "Schedule after Persist", key => Decisions.Persist<CounterCommand, CounterEvent>(new Incremented(1)).Schedule(key, TimeSpan.FromSeconds(1), new Increment(1)) },
    };

    [Theory]
    [MemberData(nameof(Asks))]
    public void A_key_that_starts_with_durable_dash_is_refused_by_every_way_to_ask_for_a_timer(
        string name, Func<string, Handled<CounterCommand, CounterEvent>> ask)
    {
        Assert.NotNull(name);
        foreach (var key in new[] { "durable-", "durable-effects-wake", "durable-retry-1", "durable-start:x" })
            Assert.Throws<ArgumentException>(() => ask(key));
    }

    [Theory]
    [MemberData(nameof(Asks))]
    public void A_key_that_only_looks_like_the_prefix_is_allowed(
        string name, Func<string, Handled<CounterCommand, CounterEvent>> ask)
    {
        Assert.NotNull(name);
        foreach (var key in new[] { "durable", "Durable-x", "my-durable-x", "durables" })
            ask(key);
    }

    [Fact]
    public void An_empty_key_is_refused()
    {
        Assert.Throws<ArgumentException>(() => None().Schedule("", TimeSpan.FromSeconds(1), new Increment(1)));
    }
}

/// <summary>Fails the write of the entries that match.</summary>
file sealed class FailWhen(Func<object, bool> predicate) : IJournalInterceptor
{
    public Task InterceptAsync(IPersistentRepresentation message) =>
        predicate(message.Payload) ? throw new InvalidOperationException("the journal is down") : Task.CompletedTask;
}

/// <summary>Lets every write through and says when an entry that matches arrives.</summary>
file sealed class RecordWhen(Func<object, bool> predicate, Action seen) : IJournalInterceptor
{
    public Task InterceptAsync(IPersistentRepresentation message)
    {
        if (predicate(message.Payload)) seen();
        return Task.CompletedTask;
    }
}

/// <summary>Writes the given entries under the persistence id of an entity.</summary>
file sealed class JournalScript : ReceivePersistentActor
{
    public JournalScript(string persistenceId)
    {
        PersistenceId = persistenceId;
        Recover<object>(_ => { });
        Command<Append>(append =>
        {
            var sequences = new List<long>();
            var sender = Sender;
            PersistAll(append.Entries, _ =>
            {
                sequences.Add(LastSequenceNr);
                if (sequences.Count == append.Entries.Length) sender.Tell(new Done([.. sequences]));
            });
        });
    }

    public override string PersistenceId { get; }

    public sealed record Append(object[] Entries);

    public sealed record Done(long[] Sequences);
}
