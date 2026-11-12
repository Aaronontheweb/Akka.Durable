using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Akka.Actor;
using Akka.Cluster.Sharding;

namespace Akka.Durable;

/// <summary>
/// Takes one new snapshot of an entity, on the watcher's actor. Returns true when the subscription is done.
/// </summary>
internal interface IStateSink
{
    bool Offer(EntityStateReply snapshot);

    /// <summary>The watcher stopped. A sink that is not done yet ends here.</summary>
    void Stopped();
}

/// <summary>
/// One state subscription, on the caller's node. It subscribes through the region, watches the entity that answers,
/// and subscribes again when that entity stops. It passes each snapshot that is newer than the last one to its sink.
/// </summary>
/// <remarks>
/// <para>A subscribe can be lost: it can reach a host just before the host learns that the entity stopped. So the
/// watcher sends it again every <see cref="SubscribeAgainEvery"/> until an entity answers. A second subscribe is
/// harmless: the entity keeps one entry for each watcher, and the watcher drops a snapshot whose sequence number is not
/// greater than the last one it passed on.</para>
/// <para>When the watcher stops, the entity gets <see cref="Terminated"/> and removes it.</para>
/// </remarks>
internal sealed class StateWatcher : ReceiveActor, IWithTimers
{
    internal static readonly TimeSpan SubscribeAgainEvery = TimeSpan.FromSeconds(1);

    private readonly IActorRef _region;
    private readonly EntityId _id;
    private readonly IStateSink _sink;

    // The entity incarnation this watcher is subscribed to. Null while it subscribes.
    private IActorRef? _entity;

    // The sequence number of the last snapshot passed to the sink. The first snapshot is always passed on.
    private long _last = -1;

    public ITimerScheduler Timers { get; set; } = null!;

    public StateWatcher(IActorRef region, EntityId id, IStateSink sink)
    {
        _region = region;
        _id = id;
        _sink = sink;

        Receive<EntityStateReply>(Take);
        Receive<Terminated>(terminated =>
        {
            if (!terminated.ActorRef.Equals(_entity)) return;
            _entity = null;
            Subscribe();
        });
        Receive<SubscribeAgain>(_ => Send());
    }

    /// <summary>Starts a watcher for one subscription. Stop it with <see cref="ActorSystem.Stop"/>.</summary>
    public static IActorRef Start(ActorSystem system, IActorRef region, EntityId id, IStateSink sink) =>
        ((ExtendedActorSystem)system).SystemActorOf(Props.Create(() => new StateWatcher(region, id, sink)));

    protected override void PreStart() => Subscribe();

    protected override void PostStop() => _sink.Stopped();

    private void Subscribe()
    {
        Send();
        Timers.StartPeriodicTimer(SubscribeAgain.Instance, SubscribeAgain.Instance, SubscribeAgainEvery);
    }

    private void Send() => _region.Tell(new ShardingEnvelope(_id.Value, SubscribeToState.Instance), Self);

    private void Take(EntityStateReply snapshot)
    {
        if (_entity is null)
        {
            // The sender is the entity itself, not the region.
            _entity = Sender;
            Context.Watch(Sender);
            Timers.Cancel(SubscribeAgain.Instance);
        }

        if (snapshot.SequenceNr <= _last) return;
        _last = snapshot.SequenceNr;

        if (_sink.Offer(snapshot)) Context.Stop(Self);
    }

    private sealed class SubscribeAgain : INoSerializationVerificationNeeded
    {
        public static readonly SubscribeAgain Instance = new();
        private SubscribeAgain() { }
    }
}

/// <summary>The wait and the stream that the refs build on a <see cref="StateWatcher"/>.</summary>
internal static class StateSubscription
{
    /// <summary>
    /// Waits for the first snapshot whose state satisfies <paramref name="until"/>. The predicate runs on the watcher,
    /// once for each snapshot that reaches it.
    /// </summary>
    /// <param name="read">Turns the entity's snapshot into the caller's. May throw: the wait then fails.</param>
    public static async Task<StateSnapshot<T>> WaitAsync<T>(
        ActorSystem system, IActorRef region, EntityId id, Func<EntityStateReply, StateSnapshot<T>> read,
        Func<T, bool> until, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(until);
        cancellationToken.ThrowIfCancellationRequested();

        var sink = new WaitSink<T>(id, read, until);
        var watcher = StateWatcher.Start(system, region, id, sink);
        try
        {
            await using (cancellationToken.Register(() => sink.Done.TrySetCanceled(cancellationToken)))
                return await sink.Done.Task.ConfigureAwait(false);
        }
        finally
        {
            system.Stop(watcher);
        }
    }

    /// <summary>
    /// The current snapshot, then each later one. A slow reader gets the newest one it has not read. The stream ends
    /// after a snapshot that satisfies <paramref name="last"/>.
    /// </summary>
    /// <param name="read">Turns the entity's snapshot into the caller's. May throw: the stream then fails.</param>
    public static async IAsyncEnumerable<StateSnapshot<T>> WatchAsync<T>(
        ActorSystem system, IActorRef region, EntityId id, Func<EntityStateReply, StateSnapshot<T>> read,
        Func<T, bool>? last, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var sink = new StreamSink<T>(id, read, last);
        var watcher = StateWatcher.Start(system, region, id, sink);
        try
        {
            await foreach (var snapshot in sink.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return snapshot;
        }
        finally
        {
            system.Stop(watcher);
        }
    }

    private static InvalidOperationException StoppedEarly(EntityId id) => new(
        $"The state subscription to entity [{id.Value}] stopped before it was done, for example because the actor " +
        "system is shutting down.");

    private sealed class WaitSink<T>(EntityId id, Func<EntityStateReply, StateSnapshot<T>> read, Func<T, bool> until)
        : IStateSink
    {
        public TaskCompletionSource<StateSnapshot<T>> Done { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool Offer(EntityStateReply reply)
        {
            try
            {
                var snapshot = read(reply);
                if (!until(snapshot.State)) return false;
                Done.TrySetResult(snapshot);
            }
            catch (Exception error)
            {
                Done.TrySetException(error);
            }

            return true;
        }

        public void Stopped() => Done.TrySetException(StoppedEarly(id));
    }

    private sealed class StreamSink<T>(EntityId id, Func<EntityStateReply, StateSnapshot<T>> read, Func<T, bool>? last)
        : IStateSink
    {
        // One item. A newer snapshot replaces the one the reader has not read yet.
        private readonly Channel<StateSnapshot<T>> _channel = Channel.CreateBounded<StateSnapshot<T>>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = true,
            });

        public ChannelReader<StateSnapshot<T>> Reader => _channel.Reader;

        public bool Offer(EntityStateReply reply)
        {
            try
            {
                var snapshot = read(reply);
                _channel.Writer.TryWrite(snapshot);
                if (last is null || !last(snapshot.State)) return false;
                _channel.Writer.TryComplete();
            }
            catch (Exception error)
            {
                _channel.Writer.TryComplete(error);
            }

            return true;
        }

        public void Stopped() => _channel.Writer.TryComplete(StoppedEarly(id));
    }
}
