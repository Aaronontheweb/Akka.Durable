using System.Collections.Concurrent;
using Akka.Durable.Tests.Fixtures;
using Akka.Reminders;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// What a caller sees from <c>WaitForStateAsync</c> and <c>WatchStateAsync</c> on an entity ref, in local and in
/// sharded hosting.
/// </summary>
public abstract class StateSubscriptionSpecs(ITestOutputHelper output) : StateSubscriptionKit(output)
{
    #region Watching: what the stream gives

    [Fact]
    public async Task A_new_entity_streams_its_initial_state_with_sequence_nr_zero()
    {
        var counter = DurableCounterRef("a");
        await using var reader = Watching(counter);

        Assert.Equal(Snap(0, 0), await reader.NextAsync());
    }

    [Fact]
    public async Task An_entity_with_history_streams_the_state_and_sequence_nr_that_GetStateAsync_returns()
    {
        var counter = DurableCounterRef("a");
        await counter.Ask(new Increment(2), AskCancellation());
        await counter.Ask(new LockCounter(), AskCancellation());
        await counter.Ask(new UnlockCounter(), AskCancellation());
        var read = await counter.GetStateAsync(AskCancellation());

        await using var reader = Watching(counter);

        var first = await reader.NextAsync();
        Assert.Equal(read, first);
        Assert.Equal(Snap(2, 3), first);
    }

    [Fact]
    public async Task Each_command_that_persists_an_event_gives_one_snapshot_equal_to_GetStateAsync()
    {
        var counter = DurableCounterRef("a");
        await using var reader = Watching(counter);
        Assert.Equal(Snap(0, 0), await reader.NextAsync());

        var seen = new List<StateSnapshot<CounterState>>();
        for (var i = 1; i <= 3; i++)
        {
            await Add(counter, 1);
            var read = await counter.GetStateAsync(AskCancellation());

            var pushed = await reader.NextAsync();

            Assert.Equal(read, pushed);
            seen.Add(pushed);
        }

        Assert.Equal([Snap(1, 1), Snap(2, 2), Snap(3, 3)], seen);
        await reader.ExpectNothingAsync(Short);
    }

    [Fact]
    public async Task A_write_that_holds_only_a_library_record_gives_no_snapshot()
    {
        var counter = DurableCounterRef("a");
        await Add(counter, 1);
        await using var reader = Watching(counter);
        Assert.Equal(Snap(1, 1), await reader.NextAsync());

        // Announce issues an effect whose handler has no answer: the entity writes library records, and no event.
        await counter.Ask(new Announce("hello"), AskCancellation());
        await AwaitConditionAsync(
            () => Task.FromResult(Recorder.Entries.Any(e => e.StartsWith("announce:hello"))),
            TimeSpan.FromSeconds(20), cancellationToken: TestCancellation);
        var afterAnnounce = await counter.GetStateAsync(AskCancellation());
        Assert.True(afterAnnounce.SequenceNr > 1, "The library records should have moved the sequence number.");
        Assert.Equal(1, afterAnnounce.State.Count);

        await reader.ExpectNothingAsync(Short);

        // The next snapshot is the one of the next event, not one for the library records.
        await Add(counter, 1);
        var next = await reader.NextAsync();
        Assert.Equal(2, next.State.Count);
        Assert.True(next.SequenceNr > afterAnnounce.SequenceNr);
    }

    [Fact]
    public async Task A_reader_that_reads_only_after_three_quick_writes_gets_the_third_state_next()
    {
        var counter = DurableCounterRef("a");
        await using var reader = Watching(counter);
        Assert.Equal(Snap(0, 0), await reader.NextAsync());

        // The reader is not reading now.
        for (var i = 0; i < 3; i++) await Add(counter, 1);
        await counter.WaitForStateAsync(s => s.Count == 3, AskCancellation());
        await ExpectNoMsgAsync(NoMsgWindow, TestCancellation);

        var next = await reader.NextAsync();
        Assert.Equal(Snap(3, 3), next);

        // Then each later state, and never an older one.
        await Add(counter, 1);
        Assert.Equal(Snap(4, 4), await reader.NextAsync());
        await reader.ExpectNothingAsync(Short);
    }

    #endregion

    #region Waiting

    [Fact]
    public async Task WaitForStateAsync_returns_an_intermediate_state_when_two_writes_follow_each_other_with_no_read_between()
    {
        var counter = DurableCounterRef("a");
        var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var slow = new ManualResetEventSlim();

        // The predicate is slow on the first state: the writes below are done before it returns.
        var wait = counter.WaitForStateAsync(state =>
        {
            if (state.Count == 0)
            {
                subscribed.TrySetResult();
                slow.Wait(TimeSpan.FromSeconds(20), TestCancellation);
            }

            return state.Count == 1;
        }, AskCancellation());
        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(30), TestCancellation);

        await Add(counter, 1);
        await Add(counter, 1);
        await Add(counter, 1);
        Assert.Equal(Snap(3, 3), await counter.GetStateAsync(AskCancellation()));
        slow.Set();

        var found = await wait;

        Assert.Equal(Snap(1, 1), found);
    }

    [Fact]
    public async Task WaitForStateAsync_returns_an_intermediate_state_when_the_writes_are_sent_back_to_back()
    {
        var counter = DurableCounterRef("a");
        var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = counter.WaitForStateAsync(state =>
        {
            if (state.Count == 0) subscribed.TrySetResult();
            return state.Count == 1;
        }, AskCancellation());
        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(30), TestCancellation);

        counter.Tell(new Increment(1), TestActor);
        counter.Tell(new Increment(1), TestActor);

        Assert.Equal(Snap(1, 1), await wait);
    }

    [Fact]
    public async Task WaitForStateAsync_returns_at_once_when_the_current_state_already_satisfies_the_predicate()
    {
        var counter = DurableCounterRef("a");
        await Add(counter, 2);
        var read = await counter.GetStateAsync(AskCancellation());

        // No command follows. A wait that needed one would run out of time.
        var found = await counter.WaitForStateAsync(s => s.Count == 2, AskCancellation(TimeSpan.FromSeconds(10)));

        Assert.Equal(read, found);
    }

    [Fact]
    public async Task WaitForStateAsync_returns_the_first_snapshot_that_a_later_command_makes_satisfy_the_predicate()
    {
        var counter = DurableCounterRef("a");
        var seen = new ConcurrentQueue<int>();
        var wait = counter.WaitForStateAsync(s =>
        {
            seen.Enqueue(s.Count);
            return s.Count >= 2;
        }, AskCancellation());
        await AwaitConditionAsync(() => Task.FromResult(seen.Contains(0)), TimeSpan.FromSeconds(30),
            cancellationToken: TestCancellation);

        await Add(counter, 1);
        await AwaitConditionAsync(() => Task.FromResult(seen.Contains(1)), TimeSpan.FromSeconds(20),
            cancellationToken: TestCancellation);
        Assert.False(wait.IsCompleted);

        await Add(counter, 1);
        var found = await wait;

        Assert.Equal(Snap(2, 2), found);
        Assert.Equal(await counter.GetStateAsync(AskCancellation()), found);
    }

    [Fact]
    public async Task WaitForStateAsync_throws_OperationCanceledException_when_its_token_fires_and_the_entity_drops_the_subscriber()
    {
        var counter = DurableCounterRef("a");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestCancellation);
        var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = counter.WaitForStateAsync(_ =>
        {
            subscribed.TrySetResult();
            return false;
        }, cts.Token);
        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(30), TestCancellation);

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        await AssertEntityStopsSendingToStoppedSubscribers(counter);
    }

    [Fact]
    public async Task WaitForStateAsync_throws_OperationCanceledException_when_the_token_is_already_cancelled()
    {
        var counter = DurableCounterRef("a");
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => counter.WaitForStateAsync(_ => true, cts.Token));
    }

    [Fact]
    public async Task WaitForStateAsync_fails_with_the_exception_of_a_predicate_that_throws_on_the_current_state()
    {
        var counter = DurableCounterRef("a");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            counter.WaitForStateAsync(_ => throw new InvalidOperationException("predicate failed"), AskCancellation()));

        Assert.Equal("predicate failed", error.Message);
    }

    [Fact]
    public async Task WaitForStateAsync_fails_with_the_exception_of_a_predicate_that_throws_on_a_later_state()
    {
        var counter = DurableCounterRef("a");
        var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = counter.WaitForStateAsync(state =>
        {
            if (state.Count == 0)
            {
                subscribed.TrySetResult();
                return false;
            }

            throw new InvalidOperationException("predicate failed later");
        }, AskCancellation());
        await subscribed.Task.WaitAsync(TimeSpan.FromSeconds(30), TestCancellation);

        await Add(counter, 1);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => wait);
        Assert.Equal("predicate failed later", error.Message);
    }

    #endregion

    #region What a subscription costs

    [Fact]
    public async Task Subscribing_writes_nothing_to_the_journal_or_the_reminder_store()
    {
        var counter = DurableCounterRef("a");
        await Add(counter, 1);
        await Add(counter, 1);
        var entity = new ReminderEntity(Counter.TypeName.Value, "a");
        var journalBefore = await JournalOf("a");
        var sequenceBefore = (await counter.GetStateAsync(AskCancellation())).SequenceNr;
        var remindersBefore = await Reminders.GetRemindersForEntityAsync(entity, 100, 0, TestCancellation);
        var writesBefore = Reminders.Writes("a");
        Assert.Equal(2, journalBefore.Length);

        await counter.WaitForStateAsync(_ => true, AskCancellation());
        await using (var reader = Watching(counter))
        {
            await reader.NextAsync();
            await reader.ExpectNothingAsync(Short);

            // While the stream is open.
            Assert.Equal(journalBefore, await JournalOf("a"));
        }

        Assert.Equal(journalBefore, await JournalOf("a"));
        Assert.Equal(sequenceBefore, (await counter.GetStateAsync(AskCancellation())).SequenceNr);
        Assert.Equal(writesBefore, Reminders.Writes("a"));
        Assert.Equal(
            remindersBefore.Count,
            (await Reminders.GetRemindersForEntityAsync(entity, 100, 0, TestCancellation)).Count);
    }

    [Fact]
    public async Task When_the_reader_of_a_stream_stops_the_entity_sends_it_nothing_more()
    {
        var counter = DurableCounterRef("a");
        var reader = Watching(counter);
        Assert.Equal(Snap(0, 0), await reader.NextAsync());
        await Add(counter, 1);
        Assert.Equal(Snap(1, 1), await reader.NextAsync());

        await reader.StopAsync();

        await AssertEntityStopsSendingToStoppedSubscribers(counter);
    }

    [Fact]
    public async Task When_the_token_of_a_stream_fires_the_entity_sends_nothing_more()
    {
        var counter = DurableCounterRef("a");
        var reader = Watching(counter);
        Assert.Equal(Snap(0, 0), await reader.NextAsync());

        await reader.DisposeAsync();

        await AssertEntityStopsSendingToStoppedSubscribers(counter);
    }

    #endregion
}

public sealed class LocalStateSubscriptionSpecs(ITestOutputHelper output) : StateSubscriptionSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedStateSubscriptionSpecs(ITestOutputHelper output) : StateSubscriptionSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>
/// The same specs with <c>VerifyMessageSerialization</c> on: every message is serialized and read back, the
/// subscription messages and the state snapshots included.
/// </summary>
public sealed class VerifiedLocalStateSubscriptionSpecs(ITestOutputHelper output) : StateSubscriptionSpecs(output)
{
    protected override bool Sharded => false;

    protected override bool VerifyMessageSerialization => true;
}

public sealed class VerifiedShardedStateSubscriptionSpecs(ITestOutputHelper output) : StateSubscriptionSpecs(output)
{
    protected override bool Sharded => true;

    protected override bool VerifyMessageSerialization => true;
}
