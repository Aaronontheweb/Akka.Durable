using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Retried effects through the public hosting API, in both hosting modes, on a reminder store the spec can watch.
/// The counter is scripted by <see cref="ProbeEntity"/>. The policies of the three effect types come from the subclass.
/// </summary>
public abstract class OutboxHostingSpec(ITestOutputHelper output) : CounterHostingSpec(output)
{
    protected static readonly TimeSpan FirstAskTimeout = TimeSpan.FromSeconds(30);
    protected static readonly TimeSpan Patience = TimeSpan.FromSeconds(20);

    protected ProbeWorld World { get; } = new();

    protected SwitchableReminderStorage Reminders { get; } = new();

    protected virtual RetryPolicy? ProbePolicy => null;

    protected virtual RetryPolicy? NotePolicy => null;

    protected virtual RetryPolicy? ComputePolicy => null;

    protected virtual EntityOptions Options => EntityOptions.Default with { EffectTimeout = TimeSpan.FromMilliseconds(500) };

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        ProbeEntity.Definition(World, Options);

    protected override IReminderStorage CreateReminderStorage(ActorSystem system) => Reminders;

    protected abstract string EntityPath(string id);

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddSingleton(World)
            .AddEffectHandler<Compute, CounterCommand, ComputeHandler>(ComputePolicy)
            .AddEffectHandler<Probe, CounterCommand, ProbeHandler>(ProbePolicy)
            .AddEffectHandler<Note, NoteHandler>(NotePolicy);

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder)
    {
        builder.WithCustomSerializer(ProbeSerializer.CreateSerializerDetails);
        if (Sharding is not null) ConfigureSingleNodeCluster(builder);
    }

    protected void Say(string id, string note) => CounterRef(id).Tell(new NoHandler(note), TestActor);

    protected async Task<CounterState> StateOf(string id) =>
        (await DurableCounterRef(id).GetStateAsync(AskCancellation(FirstAskTimeout))).State;

    /// <summary>Starts the entity and waits for the cluster, so later timings do not include them.</summary>
    protected Task Warm(string id) => StateOf(id);

    protected Task Eventually(Func<Task> check) => AwaitAssertAsync(check, Patience, cancellationToken: TestCancellation);

    protected Task AwaitProbeCalls(int count) => Eventually(() =>
    {
        Assert.Equal(count, World.ProbeCalls.Count);
        return Task.CompletedTask;
    });

    protected Task AwaitCount(string id, int count) => Eventually(async () => Assert.Equal(count, (await StateOf(id)).Count));

    /// <summary>Waits until the entity runs, without sending it anything.</summary>
    protected Task AwaitRunning(string id) => Eventually(async () => await EntityActorOf(id));

    protected async Task<IActorRef> EntityActorOf(string id) =>
        await Sys.ActorSelection(EntityPath(id)).ResolveOne(TimeSpan.FromSeconds(1), TestCancellation);

    protected async Task Kill(string id)
    {
        var entity = await EntityActorOf(id);
        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: TestCancellation);
    }

    protected async Task<IReadOnlyList<ScheduledReminder>> RemindersOf(string id)
    {
        var listed = await Sys.ReminderClient().CreateClient(Counter.TypeName.Value, id).ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    protected Task AwaitNoReminders(string id) => Eventually(async () => Assert.Empty(await RemindersOf(id)));

    protected string[] GaveUpDecisions => [.. World.Decided.Where(d => d.StartsWith("gaveup:", StringComparison.Ordinal))];

    protected int[] Attempts => [.. World.ProbeCalls.Select(c => c.Delivery.Attempt)];

    protected static Task<CounterCommand> Answer(string name) => Task.FromResult<CounterCommand>(new NoHandler($"answer:{name}"));

    protected static Task<CounterCommand> Hang(CancellationToken ct) =>
        Task.Delay(Timeout.Infinite, ct).ContinueWith<CounterCommand>(_ => default);

    /// <summary>The store of the wake is refused: the command that issues an effect fails and writes nothing.</summary>
    protected async Task AssertRefusedWakeFailsTheCommandAndTheNextOneWorks(string id, Func<Task> ask, Func<int> handlerCalls)
    {
        await Warm(id);
        Reminders.Failing = true;

        await Assert.ThrowsAnyAsync<Exception>(ask);

        Reminders.Failing = false;
        Assert.Equal(0, handlerCalls());
        Assert.Empty(await PendingEffects<CounterCommand>(id));
        Assert.Equal(new CounterState(0, false), await StateOf(id));
        Assert.Empty(await RemindersOf(id));

        // The entity is not stuck, and the same command now works and runs its handler once.
        await ask();
        await Eventually(() =>
        {
            Assert.Equal(1, handlerCalls());
            return Task.CompletedTask;
        });
    }
}

/// <summary>
/// The probe effect is UntilAcknowledged(200 ms, 400 ms). The note has no policy, nor has the compute effect.
/// The effect time-out is 500 ms.
/// </summary>
public abstract class UntilAcknowledgedHostingSpec(ITestOutputHelper output) : OutboxHostingSpec(output)
{
    protected override RetryPolicy? ProbePolicy => Retry.UntilAcknowledged(TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(400));

    [Fact]
    public async Task The_wake_brings_a_killed_entity_back_and_the_effect_is_attempted_again_with_nobody_sending_anything()
    {
        World.Hold("a");
        await Warm("woken");
        Say("woken", "issue:a");
        await AwaitProbeCalls(1);
        await Kill("woken");
        World.OnProbe = (_, _) => Answer("a"); // the next attempt answers
        World.Release("a");

        // No message, no state read: only the wake reminder can start the entity again.
        await AwaitProbeCalls(2);

        Assert.Equal([1, 2], Attempts);
        Assert.Single(World.ProbeCalls.Select(c => c.Delivery.Id).Distinct());
        Assert.StartsWith("counter/woken/", World.ProbeCalls.First().Delivery.Id.Value);
        await AwaitCount("woken", 101);
        Assert.Single(World.Decided, d => d == "answer:a");
        await AwaitNoReminders("woken");
    }

    [Fact]
    public async Task The_wake_brings_a_passivated_entity_back_in_time_for_the_next_attempt_with_nobody_sending_anything()
    {
        // Attempt 1 fails once the spec lets it. By then the entity was asked to stop, and it stops with the retry pending.
        World.Hold("a");
        World.OnProbe = (_, _) => World.ProbeCalls.Count == 1 ? throw new InvalidOperationException("not yet") : Answer("a");
        await Warm("passivated");
        Say("passivated", "issue:a");
        await AwaitProbeCalls(1);
        var entity = await EntityActorOf("passivated");
        Watch(entity);
        var stopped = CounterRef("passivated").Ask(new Shutdown(), AskCancellation());
        World.Release("a");
        await ExpectTerminatedAsync(entity, Patience, cancellationToken: TestCancellation);
        var stoppedAt = DateTimeOffset.UtcNow;

        // The retry waits 200 ms. The wake should be due about then, not many seconds later.
        await AwaitProbeCalls(2);

        Assert.True(DateTimeOffset.UtcNow - stoppedAt < TimeSpan.FromSeconds(3),
            $"The next attempt came {DateTimeOffset.UtcNow - stoppedAt} after the entity stopped, but its retry was due after 200 ms.");
        await stopped;
        await AwaitCount("passivated", 101);
    }

    [Fact]
    public async Task The_wake_is_due_at_the_earliest_attempt_also_when_a_later_effect_was_issued_first()
    {
        // The note is issued first and has no policy: its wake is due at its time-out plus a margin of seconds. The probe
        // is issued after it and is due after 700 ms. A crash leaves the probe waiting for the wake of the note.
        World.Hold("a", "b");
        await Warm("earliest");
        Say("earliest", "note:b");
        await AwaitNoteCalls(1);
        Say("earliest", "issue:a");
        await AwaitProbeCalls(1);
        await Kill("earliest");
        World.OnProbe = (_, _) => Answer("a");
        World.Release("a");
        var killedAt = DateTimeOffset.UtcNow;

        await AwaitProbeCalls(2);

        Assert.True(DateTimeOffset.UtcNow - killedAt < TimeSpan.FromSeconds(4),
            $"The probe was attempted again {DateTimeOffset.UtcNow - killedAt} after the crash, but it was due after about 700 ms.");
    }

    private Task AwaitNoteCalls(int count) => Eventually(() =>
    {
        Assert.Equal(count, World.NoteCalls.Count);
        return Task.CompletedTask;
    });

    [Fact]
    public async Task A_plain_Execute_cut_off_by_a_crash_is_decided_as_interrupted_after_the_wake_with_nobody_touching_the_entity()
    {
        World.Hold("b");
        await Warm("plain");
        Say("plain", "note:b");
        await AwaitNoteCalls(1);
        await Kill("plain");

        // The wake is due after the effect time-out plus a margin. Nobody sends a message: the entity starts by itself.
        await AwaitRunning("plain");

        await Eventually(async () => Assert.Equal("b:Interrupted:1:True", (await StateOf("plain")).LastGaveUp));
        Assert.Single(World.NoteCalls);
        Assert.Single(GaveUpDecisions);
    }

    [Fact]
    public async Task One_wake_covers_all_pending_effects_and_it_is_gone_when_the_last_one_is_settled()
    {
        World.Hold("a", "b", "c");
        await Warm("one-wake");
        Say("one-wake", "issue:a");
        Say("one-wake", "issue:b");
        Say("one-wake", "note:c");
        await AwaitProbeCalls(2);
        await AwaitNoteCalls(1);

        var wake = Assert.Single(await RemindersOf("one-wake"));
        Assert.Equal(new ReminderKey("durable-effects-wake"), wake.Key);
        World.OnProbe = (probe, _) => Answer(probe.Name);
        World.Release("a");
        await AwaitCount("one-wake", 201); // issue:a, issue:b, answer a
        Assert.Single(await RemindersOf("one-wake")); // b and c are still pending

        World.Release("b");
        World.Release("c");
        await AwaitCount("one-wake", 202);

        await AwaitNoReminders("one-wake");
        Assert.Empty(await PendingEffects<CounterCommand>("one-wake"));
    }

    [Fact]
    public async Task A_wake_is_not_left_behind_when_an_interrupted_effect_is_given_up_on_the_first_message_after_a_crash()
    {
        World.Hold("b");
        await Warm("left-behind");
        Say("left-behind", "note:b");
        await AwaitNoteCalls(1);
        await Kill("left-behind");

        // The first message starts the entity: it gives the effect up as interrupted, so nothing is pending.
        await Eventually(async () => Assert.Equal("b:Interrupted:1:True", (await StateOf("left-behind")).LastGaveUp));

        // The wake of the incarnation that crashed would only fire after its margin of seconds, and do nothing.
        await AwaitAssertAsync(async () => Assert.Empty(await RemindersOf("left-behind")),
            TimeSpan.FromSeconds(2), cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task When_the_wake_cannot_be_stored_a_plain_Execute_fails_the_command_and_writes_nothing_and_the_next_command_works()
    {
        int Calls() => Recorder.Entries.Count(e => e.StartsWith("compute:", StringComparison.Ordinal));

        await AssertRefusedWakeFailsTheCommandAndTheNextOneWorks("refused-plain",
            () => CounterRef("refused-plain").Ask(new IncrementViaService(5, Wait: false), AskCancellation()), Calls);
    }
}

public sealed class LocalUntilAcknowledgedHostingSpecs(ITestOutputHelper output) : UntilAcknowledgedHostingSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    protected override string EntityPath(string id) =>
        $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/{Uri.EscapeDataString(id)}";
}

public sealed class ShardedUntilAcknowledgedHostingSpecs(ITestOutputHelper output) : UntilAcknowledgedHostingSpec(output)
{
    protected override ShardedHosting? Sharding => new();

    protected override string EntityPath(string id) => $"/system/sharding/{Counter.TypeName.Value}/*/{id}";
}

/// <summary>
/// The probe effect is Every(200 ms, 3 attempts), the note is UntilAcknowledged(100 ms, 200 ms), the compute effect is
/// Every(200 ms, 3 attempts). The effect time-out is 500 ms.
/// </summary>
public abstract class LimitedHostingSpec(ITestOutputHelper output) : OutboxHostingSpec(output)
{
    protected override RetryPolicy? ProbePolicy => Retry.Every(TimeSpan.FromMilliseconds(200), attempts: 3);

    protected override RetryPolicy? NotePolicy => Retry.UntilAcknowledged(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200));

    protected override RetryPolicy? ComputePolicy => Retry.Every(TimeSpan.FromMilliseconds(200), attempts: 3);

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task A_handler_that_fails_n_times_is_attempted_n_plus_1_times_and_one_answer_is_decided(int failures)
    {
        World.OnProbe = (_, _) => World.ProbeCalls.Count <= failures ? throw new InvalidOperationException("not yet") : Answer("a");
        await Warm("fails-n");

        Say("fails-n", "issue:a");

        await AwaitCount("fails-n", 101);
        await ExpectNoMsgAsync(NoMsgWindow, TestCancellation);
        Assert.Equal(Enumerable.Range(1, failures + 1), Attempts);
        Assert.Single(World.ProbeCalls.Select(c => c.Delivery.Id).Distinct());
        Assert.StartsWith("counter/fails-n/", World.ProbeCalls.First().Delivery.Id.Value);
        Assert.Single(World.Decided, d => d == "answer:a");
        Assert.Empty(GaveUpDecisions);
        Assert.Empty(await PendingEffects<CounterCommand>("fails-n"));
        await AwaitNoReminders("fails-n");
    }

    [Fact]
    public async Task A_policy_that_runs_out_gives_up_once_as_retries_exhausted_and_leaves_nothing_behind()
    {
        World.OnProbe = (_, _) => throw new InvalidOperationException("never");
        await Warm("exhausted");

        Say("exhausted", "issue:a");

        await Eventually(async () => Assert.Equal("a:RetriesExhausted:3:True", (await StateOf("exhausted")).LastGaveUp));
        await ExpectNoMsgAsync(NoMsgWindow, TestCancellation);
        Assert.Equal(3, World.ProbeCalls.Count);
        Assert.Single(GaveUpDecisions);
        Assert.Equal(new CounterState(100, false, LastGaveUp: "a:RetriesExhausted:3:True"), await StateOf("exhausted"));
        Assert.Empty(await PendingEffects<CounterCommand>("exhausted"));
        await AwaitNoReminders("exhausted");
    }

    [Fact]
    public async Task Crashes_between_attempts_use_up_attempts_and_the_wake_decides_the_give_up_with_nobody_touching_the_entity()
    {
        World.OnProbe = (_, ct) => Hang(ct);
        await Warm("crashes");
        Say("crashes", "issue:a");
        await AwaitProbeCalls(1);

        for (var attempt = 2; attempt <= 3; attempt++)
        {
            await Kill("crashes");
            await AwaitProbeCalls(attempt); // the wake starts the entity, which attempts again
        }

        await Kill("crashes");

        // Attempt 3 was the last one. The wake starts the entity once more and it decides the give-up.
        await Eventually(() =>
        {
            Assert.Equal(["gaveup:a:RetriesExhausted:3:True"], GaveUpDecisions);
            return Task.CompletedTask;
        });
        Assert.Equal([1, 2, 3], Attempts);
        Assert.Single(World.ProbeCalls.Select(c => c.Delivery.Id).Distinct());
        await AwaitNoReminders("crashes");
        Assert.Equal(new CounterState(100, false, LastGaveUp: "a:RetriesExhausted:3:True"), await StateOf("crashes"));
    }

    [Fact]
    public async Task Two_effects_with_different_policies_each_follow_their_own()
    {
        // The probe has 3 attempts and always fails. The note has no limit and fails 4 times: more than the probe allows.
        World.OnProbe = (_, _) => throw new InvalidOperationException("never");
        World.OnNote = (_, _) => World.NoteCalls.Count <= 4 ? throw new InvalidOperationException("not yet") : Task.CompletedTask;
        await Warm("two-policies");

        Say("two-policies", "issue:a");
        Say("two-policies", "note:b");

        await Eventually(async () => Assert.Equal("a:RetriesExhausted:3:True", (await StateOf("two-policies")).LastGaveUp));
        await Eventually(async () => Assert.Empty(await PendingEffects<CounterCommand>("two-policies")));
        Assert.Equal(3, World.ProbeCalls.Count);
        Assert.Equal([1, 2, 3, 4, 5], World.NoteCalls.Select(c => c.Delivery.Attempt));
        Assert.Single(GaveUpDecisions);
        await AwaitNoReminders("two-policies");
    }

    [Fact]
    public async Task When_the_wake_cannot_be_stored_an_Execute_with_a_policy_fails_the_command_and_writes_nothing_and_the_next_command_works()
    {
        int Calls() => Recorder.Entries.Count(e => e.StartsWith("compute:", StringComparison.Ordinal));

        await AssertRefusedWakeFailsTheCommandAndTheNextOneWorks("refused-policy",
            () => CounterRef("refused-policy").Ask(new IncrementViaService(5, Wait: false), AskCancellation()), Calls);
    }
}

public sealed class LocalLimitedHostingSpecs(ITestOutputHelper output) : LimitedHostingSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    protected override string EntityPath(string id) =>
        $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/{Uri.EscapeDataString(id)}";
}

public sealed class ShardedLimitedHostingSpecs(ITestOutputHelper output) : LimitedHostingSpec(output)
{
    protected override ShardedHosting? Sharding => new();

    protected override string EntityPath(string id) => $"/system/sharding/{Counter.TypeName.Value}/*/{id}";
}
