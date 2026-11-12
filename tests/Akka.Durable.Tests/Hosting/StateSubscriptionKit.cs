using Akka.Actor;
using Akka.Event;
using Akka.Cluster.Hosting;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Akka.Reminders.Storage;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Shared setup for the state subscription specs on the Counter entity. Subclasses choose the hosting mode.
/// The test kit has no virtual clock, so a wait that must not end is shown with a short real window.
/// </summary>
public abstract class StateSubscriptionKit(ITestOutputHelper output) : CounterHostingSpec(output)
{
    /// <summary>Short window to show that nothing arrives.</summary>
    protected static readonly TimeSpan Short = TimeSpan.FromMilliseconds(400);

    protected abstract bool Sharded { get; }

    protected override ShardedHosting? Sharding => Sharded ? new ShardedHosting() : null;

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder)
    {
        if (Sharded) ConfigureSingleNodeCluster(builder);
    }

    // The first message of a sharded host waits for the cluster and the shard coordinator.
    protected override TimeSpan DefaultAskTimeout => TimeSpan.FromSeconds(30);

    protected SwitchableReminderStorage Reminders { get; } = new();

    protected override IReminderStorage CreateReminderStorage(ActorSystem system) => Reminders;

    protected string EntityPath(string id) => Sharded
        ? $"/system/sharding/{Counter.TypeName.Value}/*/{id}"
        : $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/{Uri.EscapeDataString(id)}";

    protected Task<IActorRef> EntityActorOf(string id) =>
        Sys.ActorSelection(EntityPath(id)).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);

    protected Task Add(IEntityRef<CounterCommand> counter, int by) =>
        counter.Ask(new Increment(by), AskCancellation());

    protected static StateSnapshot<CounterState> Snap(int count, long sequenceNr, bool locked = false) =>
        new(new CounterState(count, locked), sequenceNr);

    /// <summary>Starts a stream now: the subscription begins when the enumeration starts.</summary>
    protected StreamReader<StateSnapshot<CounterState>> Watching(
        IDurableEntityRef<CounterCommand, CounterState, CounterEvent> counter) =>
        StreamReader<StateSnapshot<CounterState>>.Open(counter.WatchStateAsync, TestCancellation);

    /// <summary>The entries the journal holds for an entity.</summary>
    protected async Task<object[]> JournalOf(string entityId)
    {
        var probe = CreateTestProbe();
        var persistenceId = $"{Counter.TypeName.Value}-{entityId}";
        var replyTo = probe.Ref;
        Sys.ActorOf(Props.Create(() => new JournalReader(persistenceId, replyTo)));
        return await probe.ExpectMsgAsync<object[]>(TimeSpan.FromSeconds(10), cancellationToken: TestCancellation);
    }

    /// <summary>
    /// After a subscriber has stopped, the entity sends it nothing more: later writes make no dead letter. The first
    /// write after the stop may still reach the stopped subscriber, because the entity learns of the stop a moment
    /// later, so the check waits for a write that makes none, and then looks at several more.
    /// </summary>
    protected async Task AssertEntityStopsSendingToStoppedSubscribers(IEntityRef<CounterCommand> counter)
    {
        var dead = CreateTestProbe();
        Sys.EventStream.Subscribe(dead.Ref, typeof(AllDeadLetters));

        await AwaitAssertAsync(async () =>
        {
            await Add(counter, 1);
            await dead.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(250), TestCancellation);
        }, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);

        for (var i = 0; i < 3; i++)
        {
            await Add(counter, 1);
            await dead.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(250), TestCancellation);
        }
    }
}

/// <summary>
/// Reads an async stream one item at a time, and can show that nothing arrives. It keeps one read pending between
/// calls, so a window that ends with nothing does not lose the next item.
/// </summary>
public sealed class StreamReader<T> : IAsyncDisposable
{
    private static readonly TimeSpan MustHappen = TimeSpan.FromSeconds(30);

    private readonly CancellationTokenSource _stop;
    private readonly IAsyncEnumerator<T> _enumerator;
    private Task<bool>? _pending;
    private bool _disposed;

    private StreamReader(Func<CancellationToken, IAsyncEnumerable<T>> open, CancellationToken testToken)
    {
        _stop = CancellationTokenSource.CreateLinkedTokenSource(testToken);
        _enumerator = open(_stop.Token).GetAsyncEnumerator(_stop.Token);
    }

    /// <summary>Opens the stream and starts reading, so the subscription starts at once.</summary>
    public static StreamReader<T> Open(Func<CancellationToken, IAsyncEnumerable<T>> open, CancellationToken testToken)
    {
        var reader = new StreamReader<T>(open, testToken);
        reader.Move();
        return reader;
    }

    private Task<bool> Move() => _pending ??= _enumerator.MoveNextAsync().AsTask();

    /// <summary>The next item. Fails when the stream ends or nothing comes in time.</summary>
    public async Task<T> NextAsync(TimeSpan? within = null)
    {
        var move = Move();
        try
        {
            await move.WaitAsync(within ?? MustHappen, _stop.Token);
        }
        catch (TimeoutException)
        {
            Assert.Fail("The stream gave no item in time.");
        }

        _pending = null;
        Assert.True(await move, "The stream ended when an item was expected.");
        return _enumerator.Current;
    }

    /// <summary>Reads until an item satisfies <paramref name="until"/>. Returns every item read, the last one included.</summary>
    public async Task<List<T>> ReadUntilAsync(Func<T, bool> until, TimeSpan? within = null)
    {
        var read = new List<T>();
        var deadline = DateTime.UtcNow + (within ?? MustHappen);
        while (true)
        {
            var left = deadline - DateTime.UtcNow;
            if (left <= TimeSpan.Zero)
                Assert.Fail($"No item satisfied the condition in time. Read: {string.Join(" | ", read)}");

            var item = await NextAsync(left);
            read.Add(item);
            if (until(item)) return read;
        }
    }

    /// <summary>Reads whatever arrives during <paramref name="window"/>.</summary>
    public async Task<List<T>> CollectForAsync(TimeSpan window)
    {
        var read = new List<T>();
        var deadline = DateTime.UtcNow + window;
        while (true)
        {
            var left = deadline - DateTime.UtcNow;
            if (left <= TimeSpan.Zero) return read;

            var move = Move();
            try
            {
                await move.WaitAsync(left, _stop.Token);
            }
            catch (TimeoutException)
            {
                return read;
            }

            _pending = null;
            Assert.True(await move, "The stream ended when it should go on.");
            read.Add(_enumerator.Current);
        }
    }

    /// <summary>Nothing arrives for <paramref name="window"/>.</summary>
    public async Task ExpectNothingAsync(TimeSpan window)
    {
        var move = Move();
        try
        {
            await move.WaitAsync(window, _stop.Token);
        }
        catch (TimeoutException)
        {
            return;
        }

        _pending = null;
        Assert.Fail(await move ? $"The stream gave an item when none was expected: {_enumerator.Current}" : "The stream ended.");
    }

    /// <summary>The stream ends on its own.</summary>
    public async Task ExpectEndAsync(TimeSpan? within = null)
    {
        var move = Move();
        try
        {
            await move.WaitAsync(within ?? MustHappen, _stop.Token);
        }
        catch (TimeoutException)
        {
            Assert.Fail("The stream did not end in time.");
        }

        _pending = null;
        Assert.False(await move, $"The stream gave another item: {_enumerator.Current}");
    }

    /// <summary>The reader stops the way a <c>break</c> does: it disposes the enumerator. No read may be pending.</summary>
    public async Task StopAsync()
    {
        Assert.Null(_pending);
        _disposed = true;
        await _enumerator.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _stop.CancelAsync();
        if (_pending is { } pending)
        {
            try
            {
                await pending;
            }
            catch (OperationCanceledException)
            {
            }
        }

        try
        {
            await _enumerator.DisposeAsync();
        }
        catch (OperationCanceledException)
        {
        }

        _stop.Dispose();
    }
}
