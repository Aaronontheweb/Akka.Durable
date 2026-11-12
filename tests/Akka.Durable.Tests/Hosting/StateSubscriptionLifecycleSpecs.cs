using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// A watched entity that fails, stops or restarts. The stream subscribes again by itself and goes on from the state
/// the entity has then.
/// </summary>
public abstract class StateSubscriptionLifecycleSpecs(ITestOutputHelper output) : StateSubscriptionKit(output)
{
    private static void AssertNeverGoesBack(IEnumerable<StateSnapshot<CounterState>> read)
    {
        var last = long.MinValue;
        foreach (var snapshot in read)
        {
            Assert.True(snapshot.SequenceNr >= last, $"A snapshot with sequence nr {snapshot.SequenceNr} came after {last}.");
            last = snapshot.SequenceNr;
        }
    }

    [Fact]
    public async Task A_write_that_the_journal_fails_gives_no_snapshot_and_the_stream_then_gives_the_recovered_state()
    {
        var counter = DurableCounterRef("a");
        await Add(counter, 1);
        await using var reader = Watching(counter);
        Assert.Equal(Snap(1, 1), await reader.NextAsync());
        var entity = await EntityActorOf("a");
        Watch(entity);

        var during = new List<StateSnapshot<CounterState>>();
        await WithJournalWrite(write => write.Fail(), async () =>
        {
            counter.Tell(new Increment(5), TestActor);
            await ExpectTerminatedAsync(entity, cancellationToken: TestCancellation);
            during.AddRange(await reader.CollectForAsync(Short));
        });

        // The entity is back, with the state of the journal: the write that failed is not in it.
        Assert.DoesNotContain(during, s => s.State.Count == 6);

        await Add(counter, 1);
        var later = await reader.ReadUntilAsync(s => s.State.Count == 2);
        Assert.Equal(Snap(2, 2), later[^1]);
        Assert.DoesNotContain(later, s => s.State.Count == 6);
        AssertNeverGoesBack(later);
    }

    [Fact]
    public async Task A_write_that_the_journal_rejects_gives_no_snapshot()
    {
        var counter = DurableCounterRef("a");
        await Add(counter, 1);
        await using var reader = Watching(counter);
        Assert.Equal(Snap(1, 1), await reader.NextAsync());

        var during = new List<StateSnapshot<CounterState>>();
        await WithJournalWrite(write => write.Reject(), async () =>
        {
            counter.Tell(new Increment(5), TestActor);
            during.AddRange(await reader.CollectForAsync(TimeSpan.FromSeconds(1)));
        });

        // If the entity restarted, the stream may give the recovered state again. It never gives the rejected one.
        Assert.All(during, s => Assert.Equal(Snap(1, 1), s));

        await Add(counter, 1);
        var later = await reader.ReadUntilAsync(s => s.State.Count == 2);
        Assert.Equal(2, later[^1].State.Count);
        Assert.True(later[^1].SequenceNr >= 2);
        Assert.DoesNotContain(later, s => s.State.Count == 6);
    }

    [Fact]
    public async Task A_watched_entity_that_decides_Stop_is_started_again_and_a_later_command_reaches_the_stream()
    {
        var counter = DurableCounterRef("a");
        await Add(counter, 1);
        await using var reader = Watching(counter);
        Assert.Equal(Snap(1, 1), await reader.NextAsync());
        var first = await EntityActorOf("a");
        Watch(first);

        await counter.Ask(new Shutdown(), AskCancellation());
        await ExpectTerminatedAsync(first, cancellationToken: TestCancellation);

        // A command from another caller, after the stop.
        await Add(DurableCounterRef("a"), 10);

        var read = await reader.ReadUntilAsync(s => s.State.Count == 11);
        Assert.Equal(Snap(11, 2), read[^1]);
        Assert.All(read.Take(read.Count - 1), s => Assert.Equal(Snap(1, 1), s));
        Assert.NotEqual(first, await EntityActorOf("a"));
    }

    [Fact]
    public async Task A_watched_entity_that_restarts_gives_the_stream_its_recovered_state_and_then_each_later_state()
    {
        var counter = DurableCounterRef("a");
        await Add(counter, 1);
        await using var reader = Watching(counter);
        Assert.Equal(Snap(1, 1), await reader.NextAsync());
        var entity = await EntityActorOf("a");

        // A command with no case in Decide: the decision throws, and the supervisor restarts the entity.
        entity.Tell(default(CounterCommand), TestActor);
        await AwaitAssertAsync(async () =>
        {
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(TestCancellation);
            attempt.CancelAfter(TimeSpan.FromMilliseconds(500));
            await counter.GetStateAsync(attempt.Token);
        }, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

        await Add(counter, 1);
        var later = await reader.ReadUntilAsync(s => s.State.Count == 2);
        Assert.Equal(Snap(2, 2), later[^1]);
        Assert.All(later.Take(later.Count - 1), s => Assert.Equal(Snap(1, 1), s));
    }

    [Fact]
    public async Task A_wait_that_subscribes_while_the_entity_is_stopping_completes_in_the_next_incarnation()
    {
        var counter = DurableCounterRef("a");
        for (var round = 1; round <= 10; round++)
        {
            await Add(counter, 1);

            // The reply to Shutdown comes before the entity stops, so the subscribe races the stop.
            await counter.Ask(new Shutdown(), AskCancellation());
            var snapshot = await counter.WaitForStateAsync(s => s.Count == round, AskCancellation());

            Assert.Equal(Snap(round, round), snapshot);
        }
    }

    [Fact]
    public async Task A_stream_that_starts_while_the_entity_is_stopping_gives_the_state_of_the_next_incarnation()
    {
        var counter = DurableCounterRef("a");
        await Add(counter, 3);

        counter.Tell(new Shutdown(), TestActor);
        await using var reader = Watching(counter);

        Assert.Equal(Snap(3, 1), await reader.NextAsync());
    }
}

public sealed class LocalStateSubscriptionLifecycleSpecs(ITestOutputHelper output) : StateSubscriptionLifecycleSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedStateSubscriptionLifecycleSpecs(ITestOutputHelper output) : StateSubscriptionLifecycleSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>A decision with a Run effect that waits on a gate.</summary>
public abstract class StateSubscriptionGatedRunSpecs(ITestOutputHelper output) : StateSubscriptionKit(output)
{
    private const int Slow = 99;

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        Counter.Definition with
        {
            Decide = (state, command, ctx) => command.Value is Increment { By: Slow } slow
                ? ctx.Persist(new Incremented(slow.By))
                    .Run<Gate>((gate, ct) => gate.WaitAsync(ct))
                    .Reply(slow, new Accepted())
                : Counter.Decide(state, command, ctx),
        };

    [Fact]
    public async Task A_write_whose_Run_effect_waits_on_a_gate_gives_no_snapshot_until_the_gate_opens()
    {
        var counter = DurableCounterRef("a");
        await using var reader = Watching(counter);
        Assert.Equal(Snap(0, 0), await reader.NextAsync());

        counter.Tell(new Increment(Slow), TestActor);

        // The event is journaled, and the Run effect has not finished.
        await reader.ExpectNothingAsync(TimeSpan.FromSeconds(1));
        await ExpectNoMsgAsync(Short, TestCancellation);

        Gate.Open();

        Assert.Equal(Snap(Slow, 1), await reader.NextAsync());
        await ExpectMsgAsync<Accepted>(cancellationToken: TestCancellation);
        Assert.Equal(Snap(Slow, 1), await counter.GetStateAsync(AskCancellation()));
    }
}

public sealed class LocalStateSubscriptionGatedRunSpecs(ITestOutputHelper output) : StateSubscriptionGatedRunSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedStateSubscriptionGatedRunSpecs(ITestOutputHelper output) : StateSubscriptionGatedRunSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>A watched entity that passivates when it is idle.</summary>
public abstract class StateSubscriptionIdlePassivationSpecs(ITestOutputHelper output) : StateSubscriptionKit(output)
{
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(1);

    // The host checks every IdleTimeout / 2, so an idle entity stops within 1.5x the timeout, and the machine may be busy.
    private static readonly TimeSpan PassivationBound = TimeSpan.FromSeconds(20);

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        Counter.Definition with { Options = EntityOptions.Default with { PassivateIdleAfter = IdleTimeout } };

    [Fact]
    public async Task A_watched_entity_that_passivates_is_started_again_and_a_command_from_another_caller_reaches_the_stream()
    {
        var counter = DurableCounterRef("a");
        await using var reader = Watching(counter);
        Assert.Equal(Snap(0, 0), await reader.NextAsync());
        var first = await EntityActorOf("a");
        Watch(first);

        await ExpectTerminatedAsync(first, PassivationBound, cancellationToken: TestCancellation);

        // A command from another caller, after the passivation.
        await Add(DurableCounterRef("a"), 1);

        // The entity may passivate again, and the stream then gives its state again. It never goes back.
        var read = await reader.ReadUntilAsync(s => s.State.Count == 1);
        Assert.Equal(Snap(1, 1), read[^1]);
        Assert.All(read.Take(read.Count - 1), s => Assert.Equal(Snap(0, 0), s));
    }
}

public sealed class LocalStateSubscriptionIdlePassivationSpecs(ITestOutputHelper output)
    : StateSubscriptionIdlePassivationSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedStateSubscriptionIdlePassivationSpecs(ITestOutputHelper output)
    : StateSubscriptionIdlePassivationSpecs(output)
{
    protected override bool Sharded => true;
}
