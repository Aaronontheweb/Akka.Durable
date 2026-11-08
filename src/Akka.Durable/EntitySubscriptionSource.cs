using Akka;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Durable;

internal static class EntitySubscriptionSource<TState, TEvent>
{
    public static Source<EntityUpdate<TState, TEvent>, NotUsed> Create(
        IEntityQueryReader<TState, TEvent> reader, ActorMaterializer materializer,
        EntityTypeName type, EntityId id, Offset? offset, TimeSpan snapshotTimeout)
    {
        long? after = offset switch
        {
            null or NoOffset => null,
            Sequence { Value: >= 0 and < long.MaxValue } sequence => sequence.Value,
            Sequence => throw new ArgumentOutOfRangeException(nameof(offset)),
            _ => throw new NotSupportedException("Per-entity subscriptions support NoOffset and Sequence offsets.")
        };

        return Source.Setup<EntityUpdate<TState, TEvent>, NotUsed>((_, _) =>
        {
            var session = new Session(reader, materializer, new EntityIdentity(type, id), after, snapshotTimeout);
            return Source.UnfoldAsync<Session, EntityUpdate<TState, TEvent>>(session, async state =>
                {
                    var next = await state.NextAsync().ConfigureAwait(false);
                    return next.HasValue
                        ? Option<(Session, EntityUpdate<TState, TEvent>)>.Create((state, next.Value))
                        : Option<(Session, EntityUpdate<TState, TEvent>)>.None;
                })
                .WatchTermination((notUsed, completed) =>
                {
                    _ = completed.ContinueWith(_ => session.Dispose(), CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                    return notUsed;
                });
        }).MapMaterializedValue(_ => NotUsed.Instance)
            .SelectError(error =>
            {
                // UnfoldAsync wraps task failures. Keep the actual cause and its own inner exception.
                while (error is AggregateException { InnerExceptions: [var inner] }) error = inner;
                return error;
            });
    }

    private sealed class Session : IDisposable
    {
        private readonly IEntityQueryReader<TState, TEvent> _reader;
        private readonly ActorMaterializer _materializer;
        private readonly EntityIdentity _publisher;
        private readonly long? _after;
        private readonly TimeSpan _snapshotTimeout;
        private readonly object _sync = new();
        private readonly CancellationTokenSource _lifetime = new();
        private readonly CancellationToken _token;
        private ISinkQueue<CommittedEvent<TEvent>>? _events;
        private UniqueKillSwitch? _kill;
        private long _cursor;
        private bool _disposed;

        public Session(IEntityQueryReader<TState, TEvent> reader, ActorMaterializer materializer,
            EntityIdentity publisher, long? after, TimeSpan snapshotTimeout)
        {
            _reader = reader;
            _materializer = materializer;
            _publisher = publisher;
            _after = after;
            _snapshotTimeout = snapshotTimeout;
            _token = _lifetime.Token;
        }

        public async Task<Option<EntityUpdate<TState, TEvent>>> NextAsync()
        {
            if (_events is null)
            {
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_token);
                deadline.CancelAfter(_snapshotTimeout);
                StateSnapshot<TState> snapshot;
                try
                {
                    snapshot = await _reader.LoadSnapshotAsync(_publisher.EntityId, deadline.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException error) when (!_token.IsCancellationRequested && deadline.IsCancellationRequested)
                {
                    throw new TimeoutException($"Loading the subscription snapshot exceeded {_snapshotTimeout}.", error);
                }
                _token.ThrowIfCancellationRequested();
                _cursor = Math.Max(_after ?? 0, snapshot.SequenceNr);
                var (kill, events) = _reader.Events(_publisher.EntityId, _cursor + 1)
                    .ViaMaterialized(KillSwitches.Single<CommittedEvent<TEvent>>(), Keep.Right)
                    .ToMaterialized(Sink.Queue<CommittedEvent<TEvent>>(), Keep.Both)
                    .Run(_materializer);
                lock (_sync)
                {
                    if (_disposed) { kill.Shutdown(); return Option<EntityUpdate<TState, TEvent>>.None; }
                    _kill = kill;
                    _events = events;
                }
                if (_after is null || _after < snapshot.SequenceNr)
                    return (EntityUpdate<TState, TEvent>)(snapshot with { Publisher = _publisher });
                // No snapshot is stored in the session; after this call only cursors and stream handles remain.
            }

            var next = await _events.PullAsync().WaitAsync(_token).ConfigureAwait(false);
            if (!next.HasValue) return Option<EntityUpdate<TState, TEvent>>.None;
            var item = next.Value;
            if (item.SequenceNr != _cursor + 1)
                throw new InvalidOperationException($"Subscription history has a gap or is out of order: expected {_cursor + 1}, got {item.SequenceNr}. Re-subscribe to load a new baseline.");
            _cursor = item.SequenceNr;
            return (EntityUpdate<TState, TEvent>)(item with { Publisher = _publisher });
        }

        public void Dispose()
        {
            lock (_sync)
            {
                if (_disposed) return;
                _disposed = true;
                _lifetime.Cancel();
                _kill?.Shutdown();
                _lifetime.Dispose();
            }
        }
    }
}
