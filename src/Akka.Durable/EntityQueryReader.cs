using Akka;
using Akka.Actor;
using Akka.Persistence;
using Akka.Persistence.Query;
using Akka.Streams.Dsl;

namespace Akka.Durable;

internal interface IEntityQueryReader<TState, TEvent>
{
    Task<StateSnapshot<TState>> LoadSnapshotAsync(EntityId id, CancellationToken cancellationToken);
    Source<CommittedEvent<TEvent>, NotUsed> Events(EntityId id, long fromSequenceNr);
}

internal sealed class EntityQueryReader<TState, TCommand, TEvent> : IEntityQueryReader<TState, TEvent>
{
    private readonly EntityDefinition<TState, TCommand, TEvent> _definition;
    private readonly IActorRef _snapshots;
    private readonly IEventsByPersistenceIdQuery _live;
    private readonly TimeSpan _timeout;

    public EntityQueryReader(ActorSystem system, EntityDefinition<TState, TCommand, TEvent> definition,
        EntityQueryOptions options)
    {
        _definition = definition;
        _timeout = options.SnapshotTimeout;
        _snapshots = Akka.Persistence.Persistence.Instance.Apply(system).SnapshotStoreFor(definition.Options?.SnapshotPluginId ?? "");
        var journal = PersistenceQuery.Get(system).ReadJournalFor<IReadJournal>(options.ReadJournalPluginId);
        _live = journal as IEventsByPersistenceIdQuery
            ?? throw new ArgumentException("The read journal must support EventsByPersistenceId.", nameof(options));
    }

    private string PersistenceId(EntityId id) =>
        (_definition.Options ?? EntityOptions.Default).PersistenceIdFactory(id, _definition.TypeName);

    public async Task<StateSnapshot<TState>> LoadSnapshotAsync(EntityId id, CancellationToken cancellationToken)
    {
        var response = await _snapshots.Ask<object>(
            new LoadSnapshot(PersistenceId(id), SnapshotSelectionCriteria.Latest, long.MaxValue),
            _timeout, cancellationToken).ConfigureAwait(false);

        return response switch
        {
            LoadSnapshotResult { Snapshot: null } => new(_definition.InitialState(id), 0),
            LoadSnapshotResult { Snapshot: { Snapshot: TState state } selected } =>
                new(state, selected.Metadata.SequenceNr),
            LoadSnapshotFailed failed => throw new InvalidOperationException(
                $"Failed to load the snapshot of {_definition.TypeName.Value}/{id.Value}.", failed.Cause),
            _ => throw new InvalidOperationException(
                $"Invalid snapshot response for {_definition.TypeName.Value}/{id.Value}: {response.GetType()}.")
        };
    }

    public Source<CommittedEvent<TEvent>, NotUsed> Events(EntityId id, long fromSequenceNr)
    {
        var pid = PersistenceId(id);
        var source = _live.EventsByPersistenceId(pid, fromSequenceNr, long.MaxValue);
        return source.Select(envelope => envelope.Event is TEvent @event
            ? new CommittedEvent<TEvent>(@event, envelope.SequenceNr)
            : throw new InvalidOperationException(
                $"Journal entry {pid}/{envelope.SequenceNr} is {envelope.Event.GetType()}, expected {typeof(TEvent)}."));
    }
}
