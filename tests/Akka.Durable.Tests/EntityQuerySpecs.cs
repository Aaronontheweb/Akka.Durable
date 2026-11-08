using System.Threading.Channels;
using Akka;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting.TestKit;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Xunit;

namespace Akka.Durable.Tests;

public sealed class EntityQuerySpecs(ITestOutputHelper output) : PersistenceTestKit(output: output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly EntityId Id = new("publisher");
    private readonly ControlledReader _reader = new();

    private Source<EntityUpdate<CounterState, CounterEvent>, NotUsed> Updates(Offset? offset = null,
        TimeSpan? snapshotTimeout = null) => EntitySubscriptionSource<CounterState, CounterEvent>.Create(
        _reader, Sys.Materializer(), Counter.TypeName, Id, offset, snapshotTimeout ?? TimeSpan.FromSeconds(10));

    private (UniqueKillSwitch Kill, ISinkQueue<EntityUpdate<CounterState, CounterEvent>> Queue) Subscribe(long? after = null) =>
        Materialize(Updates(after is { } sequence ? Offset.Sequence(sequence) : Offset.NoOffset()));

    private (UniqueKillSwitch Kill, ISinkQueue<EntityUpdate<CounterState, CounterEvent>> Queue) Materialize(
        Source<EntityUpdate<CounterState, CounterEvent>, NotUsed> source) => source
        .ViaMaterialized(KillSwitches.Single<EntityUpdate<CounterState, CounterEvent>>(), Keep.Right)
        .ToMaterialized(Sink.Queue<EntityUpdate<CounterState, CounterEvent>>(), Keep.Both)
        .Run(Sys.Materializer());

    private static async Task<EntityUpdate<CounterState, CounterEvent>> Next(
        ISinkQueue<EntityUpdate<CounterState, CounterEvent>> queue) =>
        (await queue.PullAsync().WaitAsync(TimeSpan.FromSeconds(10), Ct)).Value;

    private static async Task<long> NextEvent(ISinkQueue<EntityUpdate<CounterState, CounterEvent>> queue) =>
        Assert.IsType<CommittedEvent<CounterEvent>>((await Next(queue)).Value).SequenceNr;

    [Fact]
    public async Task Fresh_query_emits_initial_state_history_and_live_events_with_publisher_identity()
    {
        _reader.Append(1);
        _reader.Append(2);
        var subscription = Subscribe();
        try
        {
            var snapshot = Assert.IsType<StateSnapshot<CounterState>>((await Next(subscription.Queue)).Value);
            Assert.Equal(new CounterState(0, false), snapshot.State);
            Assert.Equal(0, snapshot.SequenceNr);
            Assert.Equal(new EntityIdentity(Counter.TypeName, Id), snapshot.Publisher);
            Assert.Equal(1, await NextEvent(subscription.Queue));
            Assert.Equal(2, await NextEvent(subscription.Queue));
            await AwaitAssertAsync(() => Assert.Equal(1, _reader.ActiveLive), cancellationToken: Ct);
            _reader.Append(3);
            var item = Assert.IsType<CommittedEvent<CounterEvent>>((await Next(subscription.Queue)).Value);
            Assert.Equal(3, item.SequenceNr);
            Assert.Equal(new Incremented(3), item.Event.Value);
            Assert.Equal(snapshot.Publisher, item.Publisher);
        }
        finally { subscription.Kill.Shutdown(); }
        await AwaitAssertAsync(() => Assert.Equal(0, _reader.ActiveLive), cancellationToken: Ct);
    }

    [Theory]
    [InlineData(null, true, 4)]
    [InlineData(1L, true, 4)]
    [InlineData(3L, false, 4)]
    [InlineData(4L, false, 5)]
    public async Task Resume_below_snapshot_resets_to_that_baseline_otherwise_starts_after_cursor(
        long? after, bool expectsSnapshot, long firstEvent)
    {
        for (var i = 1; i <= 5; i++) _reader.Append(i);
        _reader.Snapshot = _ => Task.FromResult(new StateSnapshot<CounterState>(new CounterState(6, false), 3));
        var subscription = Subscribe(after);
        try
        {
            if (expectsSnapshot)
            {
                var snapshot = Assert.IsType<StateSnapshot<CounterState>>((await Next(subscription.Queue)).Value);
                Assert.Equal(3, snapshot.SequenceNr);
                Assert.Equal(6, snapshot.State.Count);
            }
            Assert.Equal(firstEvent, await NextEvent(subscription.Queue));
        }
        finally { subscription.Kill.Shutdown(); }
    }

    [Fact]
    public async Task Different_offsets_have_independent_readers_and_each_join_loads_a_fresh_snapshot()
    {
        for (var i = 1; i <= 5; i++) _reader.Append(i);
        _reader.Snapshot = _ => Task.FromResult(new StateSnapshot<CounterState>(new CounterState(3, false), 2));
        var first = Subscribe();
        var second = default((UniqueKillSwitch Kill, ISinkQueue<EntityUpdate<CounterState, CounterEvent>> Queue));
        try
        {
            Assert.Equal(2, Assert.IsType<StateSnapshot<CounterState>>((await Next(first.Queue)).Value).SequenceNr);
            for (var i = 3; i <= 5; i++) Assert.Equal(i, await NextEvent(first.Queue));
            await AwaitAssertAsync(() => Assert.Equal(1, _reader.ActiveLive), cancellationToken: Ct);
            _reader.Snapshot = _ => Task.FromResult(new StateSnapshot<CounterState>(new CounterState(10, false), 4));
            second = Subscribe(1);
            Assert.Equal(4, Assert.IsType<StateSnapshot<CounterState>>((await Next(second.Queue)).Value).SequenceNr);
            Assert.Equal(5, await NextEvent(second.Queue));
            _reader.Append(6);
            Assert.Equal(6, await NextEvent(first.Queue));
            Assert.Equal(6, await NextEvent(second.Queue));
            Assert.Equal(2, _reader.SnapshotLoads);
            Assert.Equal(2, _reader.LiveStarts);
            first.Kill.Shutdown();
            _reader.Append(7);
            Assert.Equal(7, await NextEvent(second.Queue));
            await AwaitAssertAsync(() => Assert.Equal(1, _reader.ActiveLive), cancellationToken: Ct);
        }
        finally { first.Kill.Shutdown(); second.Kill?.Shutdown(); }
        await AwaitAssertAsync(() => Assert.Equal(0, _reader.ActiveLive), cancellationToken: Ct);
    }

    [Fact]
    public async Task A_future_checkpoint_does_not_hide_existing_history_from_a_later_consumer()
    {
        for (var i = 1; i <= 3; i++) _reader.Append(i);
        var future = Subscribe(after: 5);
        var fresh = default((UniqueKillSwitch Kill, ISinkQueue<EntityUpdate<CounterState, CounterEvent>> Queue));
        try
        {
            await AwaitAssertAsync(() => Assert.Equal(1, _reader.ActiveLive), cancellationToken: Ct);
            fresh = Subscribe();
            Assert.IsType<StateSnapshot<CounterState>>((await Next(fresh.Queue)).Value);
            for (var i = 1; i <= 5; i++)
            {
                if (i > 3) _reader.Append(i);
                Assert.Equal(i, await NextEvent(fresh.Queue));
            }
            _reader.Append(6);
            Assert.Equal(6, await NextEvent(future.Queue));
            Assert.Equal(6, await NextEvent(fresh.Queue));
            Assert.Equal(2, _reader.LiveStarts);
        }
        finally { future.Kill.Shutdown(); fresh.Kill?.Shutdown(); }
    }

    [Fact]
    public async Task Events_arriving_during_snapshot_load_are_bridged_without_gaps_or_duplicates()
    {
        _reader.Append(1);
        var first = Subscribe();
        var gate = new TaskCompletionSource<StateSnapshot<CounterState>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = default((UniqueKillSwitch Kill, ISinkQueue<EntityUpdate<CounterState, CounterEvent>> Queue));
        try
        {
            await Next(first.Queue);
            Assert.Equal(1, await NextEvent(first.Queue));
            await AwaitAssertAsync(() => Assert.Equal(1, _reader.ActiveLive), cancellationToken: Ct);
            _reader.Snapshot = ct => { entered.TrySetResult(); return gate.Task.WaitAsync(ct); };
            second = Subscribe();
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            _reader.Append(2);
            _reader.Append(3);
            Assert.Equal(2, await NextEvent(first.Queue));
            Assert.Equal(3, await NextEvent(first.Queue));
            gate.SetResult(new(new CounterState(0, false), 0));
            Assert.IsType<StateSnapshot<CounterState>>((await Next(second.Queue)).Value);
            for (var i = 1; i <= 3; i++) Assert.Equal(i, await NextEvent(second.Queue));
            _reader.Append(4);
            Assert.Equal(4, await NextEvent(second.Queue));
            Assert.Equal(2, _reader.LiveStarts);
        }
        finally { gate.TrySetCanceled(Ct); first.Kill.Shutdown(); second.Kill?.Shutdown(); }
    }

    [Fact]
    public async Task A_slow_consumer_can_backpressure_its_query_without_stalling_another_consumer()
    {
        var fast = Subscribe();
        var slow = Subscribe();
        try
        {
            await Next(fast.Queue);
            await Next(slow.Queue);
            await AwaitAssertAsync(() => Assert.Equal(2, _reader.ActiveLive), cancellationToken: Ct);
            for (var i = 1; i <= 50; i++)
            {
                _reader.Append(i);
                Assert.Equal(i, await NextEvent(fast.Queue));
            }
            for (var i = 1; i <= 50; i++) Assert.Equal(i, await NextEvent(slow.Queue));
            Assert.Equal(2, _reader.ActiveLive);
        }
        finally { fast.Kill.Shutdown(); slow.Kill.Shutdown(); }
    }

    [Fact]
    public async Task An_idle_live_subscription_does_not_retain_the_emitted_snapshot_state()
    {
        WeakReference<CounterState>? stateReference = null;
        _reader.Snapshot = _ =>
        {
            var state = new CounterState(42, false);
            stateReference = new(state);
            return Task.FromResult(new StateSnapshot<CounterState>(state, 0));
        };
        var subscription = Subscribe();
        try
        {
            await ConsumeBaseline(subscription.Queue);
            await AwaitAssertAsync(() => Assert.Equal(1, _reader.ActiveLive), cancellationToken: Ct);
            await AwaitAssertAsync(() =>
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                Assert.NotNull(stateReference);
                Assert.False(stateReference.TryGetTarget(out _), "The active query retained its bootstrap state.");
            }, cancellationToken: Ct);
        }
        finally { subscription.Kill.Shutdown(); }
    }

    private static async Task ConsumeBaseline(ISinkQueue<EntityUpdate<CounterState, CounterEvent>> queue) =>
        Assert.IsType<StateSnapshot<CounterState>>((await Next(queue)).Value);

    [Fact]
    public async Task Cancelling_during_bootstrap_releases_the_read_and_allows_another_subscription()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _reader.Snapshot = async ct =>
        {
            entered.TrySetResult();
            try { return await new TaskCompletionSource<StateSnapshot<CounterState>>().Task.WaitAsync(ct); }
            finally { cancelled.TrySetResult(); }
        };
        var first = Subscribe();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        first.Kill.Shutdown();
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        _reader.Snapshot = _ => Task.FromResult(new StateSnapshot<CounterState>(new CounterState(0, false), 0));
        var second = Subscribe();
        try { Assert.IsType<StateSnapshot<CounterState>>((await Next(second.Queue)).Value); }
        finally { second.Kill.Shutdown(); }
    }

    [Fact]
    public async Task A_query_failure_is_observable_and_a_later_subscription_can_start_again()
    {
        var first = Subscribe();
        try
        {
            await Next(first.Queue);
            await AwaitAssertAsync(() => Assert.Equal(1, _reader.ActiveLive), cancellationToken: Ct);
            _reader.FailLive(new InvalidOperationException("read journal unavailable"));
            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Next(first.Queue));
            Assert.Equal("read journal unavailable", error.Message);
        }
        finally { first.Kill.Shutdown(); }
        await AwaitAssertAsync(() => Assert.Equal(0, _reader.ActiveLive), cancellationToken: Ct);
        var second = Subscribe();
        try
        {
            await Next(second.Queue);
            _reader.Append(1);
            Assert.Equal(1, await NextEvent(second.Queue));
            await AwaitAssertAsync(() => Assert.Equal(2, _reader.LiveStarts), cancellationToken: Ct);
        }
        finally { second.Kill.Shutdown(); }
    }

    [Theory]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    public void Invalid_resume_positions_are_rejected(long after) => Assert.Throws<ArgumentOutOfRangeException>(() =>
        Updates(Offset.Sequence(after)));

    [Fact]
    public void Offset_kinds_without_per_entity_sequence_semantics_are_rejected()
    {
        Assert.Throws<NotSupportedException>(() => Updates(Offset.FromEnd(1)));
        Assert.Throws<NotSupportedException>(() => Updates(Offset.TimeBasedUuid(
            new Guid("d5f5f740-3f8b-11e7-9598-0800200c9a66"))));
    }

    [Fact]
    public async Task A_cold_source_can_be_materialized_twice_without_sharing_query_state()
    {
        var source = Updates(Offset.NoOffset());
        Assert.Equal(0, _reader.SnapshotLoads);
        Assert.Equal(0, _reader.LiveStarts);
        var first = Materialize(source);
        var second = Materialize(source);
        try
        {
            Assert.IsType<StateSnapshot<CounterState>>((await Next(first.Queue)).Value);
            Assert.IsType<StateSnapshot<CounterState>>((await Next(second.Queue)).Value);
            Assert.Equal(2, _reader.SnapshotLoads);
            await AwaitAssertAsync(() => Assert.Equal(2, _reader.ActiveLive), cancellationToken: Ct);
        }
        finally { first.Kill.Shutdown(); second.Kill.Shutdown(); }
        await AwaitAssertAsync(() => Assert.Equal(0, _reader.ActiveLive), cancellationToken: Ct);
    }

    [Fact]
    public async Task Snapshot_load_failures_preserve_their_cause_and_do_not_start_event_queries()
    {
        var failure = new InvalidOperationException("snapshot unavailable", new IOException("storage unavailable"));
        _reader.Snapshot = _ => Task.FromException<StateSnapshot<CounterState>>(failure);
        var subscription = Subscribe();
        try
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Next(subscription.Queue));
            Assert.Same(failure, error);
            Assert.Equal(0, _reader.LiveStarts);
        }
        finally { subscription.Kill.Shutdown(); }
    }

    [Fact]
    public async Task A_snapshot_timeout_fails_the_stream_and_cancels_the_load()
    {
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _reader.Snapshot = async ct =>
        {
            try { return await new TaskCompletionSource<StateSnapshot<CounterState>>().Task.WaitAsync(ct); }
            finally { cancelled.TrySetResult(); }
        };
        var subscription = Materialize(Updates(snapshotTimeout: TimeSpan.FromMilliseconds(100)));
        try
        {
            await Assert.ThrowsAsync<TimeoutException>(async () => await Next(subscription.Queue));
            await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            Assert.Equal(0, _reader.LiveStarts);
        }
        finally { subscription.Kill.Shutdown(); }
    }

    private sealed class ControlledReader : IEntityQueryReader<CounterState, CounterEvent>
    {
        private readonly object _sync = new();
        private readonly List<CommittedEvent<CounterEvent>> _events = [];
        private readonly List<(Channel<CommittedEvent<CounterEvent>> Channel, long From)> _live = [];
        private int _activeLive;
        private int _liveStarts;
        private int _snapshotLoads;
        public int ActiveLive => Volatile.Read(ref _activeLive);
        public int LiveStarts => Volatile.Read(ref _liveStarts);
        public int SnapshotLoads => Volatile.Read(ref _snapshotLoads);
        public Func<CancellationToken, Task<StateSnapshot<CounterState>>> Snapshot { get; set; } =
            _ => Task.FromResult(new StateSnapshot<CounterState>(new CounterState(0, false), 0));

        public Task<StateSnapshot<CounterState>> LoadSnapshotAsync(EntityId id, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _snapshotLoads);
            return Snapshot(cancellationToken);
        }

        public void Append(int by)
        {
            lock (_sync)
            {
                var item = new CommittedEvent<CounterEvent>(new Incremented(by), _events.Count + 1);
                _events.Add(item);
                foreach (var live in _live)
                    if (item.SequenceNr >= live.From) Assert.True(live.Channel.Writer.TryWrite(item));
            }
        }

        public void FailLive(Exception error)
        {
            lock (_sync) foreach (var live in _live) live.Channel.Writer.TryComplete(error);
        }

        public Source<CommittedEvent<CounterEvent>, NotUsed> Events(EntityId id, long fromSequenceNr) =>
            Source.Setup<CommittedEvent<CounterEvent>, NotUsed>((_, _) =>
            {
                var channel = Channel.CreateUnbounded<CommittedEvent<CounterEvent>>();
                var live = (channel, fromSequenceNr);
                lock (_sync)
                {
                    foreach (var item in _events.Where(e => e.SequenceNr >= fromSequenceNr)) channel.Writer.TryWrite(item);
                    _live.Add(live);
                }
                Interlocked.Increment(ref _activeLive);
                Interlocked.Increment(ref _liveStarts);
                return Source.UnfoldAsync<Channel<CommittedEvent<CounterEvent>>, CommittedEvent<CounterEvent>>(channel,
                    async state => await state.Reader.WaitToReadAsync() && state.Reader.TryRead(out var item)
                        ? Option<(Channel<CommittedEvent<CounterEvent>>, CommittedEvent<CounterEvent>)>.Create((state, item))
                        : Option<(Channel<CommittedEvent<CounterEvent>>, CommittedEvent<CounterEvent>)>.None)
                    .WatchTermination((notUsed, completed) =>
                    {
                        _ = completed.ContinueWith(_ =>
                        {
                            lock (_sync) _live.Remove(live);
                            channel.Writer.TryComplete();
                            Interlocked.Decrement(ref _activeLive);
                        }, TaskScheduler.Default);
                        return notUsed;
                    });
            }).MapMaterializedValue(_ => NotUsed.Instance);
    }
}
