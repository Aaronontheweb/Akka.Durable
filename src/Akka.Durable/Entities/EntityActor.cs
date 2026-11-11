using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Event;
using Akka.Hosting;
using Akka.Persistence;
using Akka.Reminders;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable;

/// <summary>
/// Generic event-sourced host for an <see cref="EntityDefinition{TState,TCommand,TEvent}"/>.
/// Applications describe entities with Decide / Evolve; they do not subclass this actor.
/// </summary>
/// <remarks>
/// Runs unchanged under a local entity host or a Cluster.Sharding shard region.
/// </remarks>
internal sealed class EntityActor<TState, TCommand, TEvent> : ReceivePersistentActor
{
    private readonly EntityId _entityId;
    private readonly EntityDefinition<TState, TCommand, TEvent> _definition;
    private readonly EntityOptions _options;
    private readonly IServiceProvider _services;
    private readonly ITimeProvider _timeProvider;
    private readonly ILoggingAdapter _log;

    // Cancelled when this incarnation stops. In-flight Run and Execute work is asked to stop, and an outcome that arrives
    // afterwards is dropped: the next incarnation gives a recorded effect up as interrupted.
    private readonly CancellationTokenSource _lifetime = new();

    // Where each effect type is handled. Registered with AddEffectHandler.
    private readonly EffectRoutes _routes;

    // Recorded effects that were issued and have no acknowledgement or give-up yet, by id. Kept by the persist handlers
    // and rebuilt in recovery from the journal records and the snapshot.
    private readonly SortedDictionary<long, PendingEffect> _pending = [];

    // Created lazily so entities that never touch reminders don't require them to be configured.
    private IReminderClient? _reminderClient;

    private long _lastSnapshotSequenceNr;

    // Execute handlers this incarnation started whose outcome it has not received yet. Send is not counted:
    // a send has no time limit. Run is awaited in place, so a stop waits for it anyway.
    private int _executesInFlight;

    // The host asked this entity to stop while an Execute handler was in flight. It stops at the end of the
    // first decision that leaves nothing in flight.
    private bool _stopRequested;

    // Starts of workflows this incarnation has issued and not had answered, by the key of the reminder that keeps
    // them. While one is in flight, that reminder does not issue the start again.
    private readonly Dictionary<string, int> _startsInFlight = [];

    // Starts whose reminder this incarnation removed, after an answer or a give-up. A delivery of that reminder that
    // was already on its way is stale: it must not issue the start again.
    private readonly HashSet<string> _startsSettled = [];

    public EntityActor(
        EntityId entityId,
        EntityDefinition<TState, TCommand, TEvent> definition,
        IServiceProvider services,
        ITimeProvider timeProvider)
    {
        _entityId = entityId;
        _definition = definition;
        _services = services;
        _timeProvider = timeProvider;
        _options = definition.Options ?? EntityOptions.Default;
        _routes = services.GetService<EffectRoutes>() ?? EffectRoutes.Empty;

        _log = Context.GetLogger()
            .WithContext("EntityType", definition.TypeName.Value)
            .WithContext("EntityId", entityId.Value);

        PersistenceId = _options.PersistenceIdFactory(entityId, definition.TypeName);
        if (_options.JournalPluginId is not null) JournalPluginId = _options.JournalPluginId;
        if (_options.SnapshotPluginId is not null) SnapshotPluginId = _options.SnapshotPluginId;

        State = definition.InitialState(entityId);

        Recovers();
        Commands();
    }

    public override string PersistenceId { get; }

    public TState State { get; private set; }

    private EntityTypeName EntityType => _definition.TypeName;

    private IReminderClient Reminders =>
        _reminderClient ??= Context.ReminderClient().CreateClient(EntityType.Value, _entityId.Value);

    private DecisionContext<TState, TCommand, TEvent> NewContext() =>
        new(_entityId, EntityType, _timeProvider.Now);

    #region Recovery

    private void Recovers()
    {
        Recover<SnapshotOffer>(offer =>
        {
            switch (offer.Snapshot)
            {
                case EntitySnapshot { State: TState state } snapshot:
                    State = state;
                    _pending.Clear();
                    foreach (var pending in snapshot.Pending.IsDefault ? [] : snapshot.Pending.Cast<PendingEffect>())
                        _pending[pending.Sequence] = pending;
                    break;

                // Written before effects were recorded: nothing is pending.
                case TState state:
                    State = state;
                    break;

                default:
                    // Ignoring the snapshot is unsafe: Akka.Persistence resumes event replay AFTER the
                    // snapshot's sequence number, so every event the snapshot covered would be silently lost.
                    var found = offer.Snapshot is EntitySnapshot wrapped ? wrapped.State?.GetType() : offer.Snapshot?.GetType();
                    throw new EntityRecoveryException(
                        $"Entity [{PersistenceId}] cannot recover: snapshot at sequence number " +
                        $"[{offer.Metadata.SequenceNr}] holds a state of type [{found}] but the expected " +
                        $"state type is [{typeof(TState)}]. The snapshot cannot be skipped, because events it covers " +
                        "are not replayed. If the entity's events are still in the journal, delete the snapshot so " +
                        "the entity recovers from its events.");
            }

            _lastSnapshotSequenceNr = offer.Metadata.SequenceNr;
            _log.Debug("Recovered snapshot at SeqNr {0}", offer.Metadata.SequenceNr);
        });

        // The library's records rebuild the pending effects. Registered before TEvent, so they never reach Evolve.
        // Akka.Persistence sets LastSequenceNr to an entry's number before its handler runs: it is the effect's id.
        Recover<EffectIssued>(issued =>
            _pending[LastSequenceNr] = new PendingEffect(LastSequenceNr, issued.Effect, issued.IssuedAt));
        Recover<EffectAcknowledged>(acknowledged => _pending.Remove(acknowledged.Id));
        Recover<EffectGaveUp>(gaveUp => _pending.Remove(gaveUp.Id));

        // Replay only domain events through Evolve. Decide and effects never run during recovery.
        // A C# 15 union TEvent comes back from the journal as the union, because WithDurableEntity requires
        // it to have its own serializer (a union marked [AkkaSerializable(Manifest = ...)]).
        Recover<TEvent>(Apply);

        Recover<RecoveryCompleted>(_ =>
        {
            _log.Debug("Recovery completed at SeqNr {0}", LastSequenceNr);
            GiveUpInterruptedEffects();
        });

        // Catch-all, registered last (handlers match in registration order). An unmatched journal entry
        // is otherwise skipped silently by Akka.Persistence, leaving the entity with missing state.
        Recover<object>(HandleUnrecognizedJournalEntry);
    }

    private void HandleUnrecognizedJournalEntry(object entry)
    {
        var problem =
            $"Entity [{PersistenceId}] journal entry at sequence number [{LastSequenceNr}] is of type " +
            $"[{entry.GetType()}] but the expected event type is [{typeof(TEvent)}].";

        if (_options.SkipUnrecognizedJournalEntries)
        {
            // Opted in: keep the entity available, but never quietly.
            _log.Error("{0} Skipping it because EntityOptions.SkipUnrecognizedJournalEntries is enabled.", problem);
            return;
        }

        throw new EntityRecoveryException(
            $"{problem} Recovery stops rather than continue with missing state. To repair or drop such " +
            "entries, bind an IReadEventAdapter to the journal; to skip them, enable " +
            "EntityOptions.SkipUnrecognizedJournalEntries.");
    }

    private void Apply(TEvent @event) => State = _definition.Evolve(State, @event);

    /// <summary>
    /// After recovery, every recorded effect that is still pending was interrupted: the previous incarnation stopped
    /// before its answer or give-up was journaled. It is not run again. Its give-up with
    /// <see cref="GaveUpReason.Interrupted"/> is decided, one effect after the other in the order they were issued, each
    /// with its record in the decision's write. Then <c>Resume</c> runs.
    /// </summary>
    /// <remarks>
    /// This all happens before the internal stash is released, so before any command that arrived during recovery.
    /// Each decision is persisted inside the completion of the one before, so each sees the state the one before left.
    /// </remarks>
    private void GiveUpInterruptedEffects()
    {
        if (_pending.Count == 0)
        {
            ResumePendingEffects();
            return;
        }

        _log.Info("{0} effects of entity [{1}/{2}] were interrupted by a restart. Each gives up as Interrupted; none is run again.",
            _pending.Count, EntityType.Value, _entityId.Value);
        GiveUpInterrupted([.. _pending.Values], 0);
    }

    private void GiveUpInterrupted(List<PendingEffect> interrupted, int index)
    {
        if (index == interrupted.Count)
        {
            ResumePendingEffects();
            return;
        }

        var pending = interrupted[index];
        var record = new EffectGaveUp(pending.Sequence, 1, GaveUpReason.Interrupted);
        Action next = () => GiveUpInterrupted(interrupted, index + 1);

        // The asker of the request that issued the effect belongs to the previous incarnation.
        var origin = Context.System.DeadLetters;
        if (pending.Effect is not IEffect<TCommand> effect)
        {
            // The effect's type changed since it was journaled. Decide cannot get its give-up, so it is only closed.
            _log.Error("Entity [{0}/{1}] has the interrupted effect {2} (id {3}), which is not an IEffect<{4}>. Decide " +
                       "cannot get its give-up. It is closed as interrupted.",
                EntityType.Value, _entityId.Value, pending.Effect.GetType().Name, pending.Sequence, typeof(TCommand).Name);
            Commit(Decisions.None<TCommand, TEvent>(), origin, reminder: null, [record], next);
            return;
        }

        var command = effect.GaveUp(new WorkGaveUp(pending.Effect, GaveUpReason.Interrupted));
        HandleCommand(command, origin, reminder: null, settlement: new Settlement(pending.Sequence, record, next));
    }

    /// <summary>
    /// Re-issues the effects the recovered state says are still pending. Committing here, before the
    /// internal stash is released, makes them run before any command that arrived during recovery.
    /// </summary>
    private void ResumePendingEffects()
    {
        if (_definition.Resume is null) return;

        var pending = _definition.Resume(State, NewContext());
        if (!pending.Events.IsDefaultOrEmpty)
            throw new InvalidOperationException(
                $"{EntityType.Value}.Resume returned events for entity [{_entityId.Value}]. Resume lists the effects " +
                "that are still pending for the recovered state; it decides nothing, so it cannot persist. " +
                "Return the events from Decide instead.");

        if (pending.Effects.IsDefaultOrEmpty) return;

        foreach (var effect in pending.Effects)
            if (effect is ExecuteEffect { Recorded: true } execute)
                throw new ExecuteFromResumeException(EntityType, _entityId, execute.Effect);

        _log.Debug("Resuming {0} pending effects", pending.Effects.Length);
        // The asker of the request that started this work belongs to the previous incarnation.
        Commit(pending, Context.System.DeadLetters, reminder: null);
    }

    #endregion

    #region Command processing

    private void Commands()
    {
        // Must precede Command<TCommand> so reminder envelopes are never treated as commands.
        Command<ReminderEnvelope>(HandleReminder);

        // State query, handled outside TCommand so Decide never sees it. Akka.Persistence stashes
        // incoming messages while events persist and Run effects execute, so a query sent after a command is
        // answered only once that command's events are committed and its Run effects have finished.
        // Must precede Command<TCommand> for the same reason as ReminderEnvelope.
        Command<GetEntityState>(_ => Sender.Tell(new EntityStateReply(State!, LastSequenceNr)));
        Command<GetPendingEffects>(_ =>
            Sender.Tell(new PendingEffectsReply([.. _pending.Values.Select(pending => pending.Effect)])));

        Command<TCommand>(command => HandleCommand(command, Sender, reminder: null));
        Command<EffectCompleted>(HandleEffectCompleted);
        Command<DecideAgain>(HandleDecideAgain);
        Command<ForeignReply>(reply => HandleForeign(reply.Message, reply.Origin));
        Command<StopEntity>(_ => RequestStop());

        Command<SaveSnapshotSuccess>(HandleSnapshotSaved);
        Command<SaveSnapshotFailure>(f =>
            _log.Warning(f.Cause, "Failed to save snapshot at SeqNr {0}", f.Metadata.SequenceNr));
        Command<DeleteSnapshotsSuccess>(_ => { });
        Command<DeleteSnapshotsFailure>(f =>
            _log.Warning(f.Cause, "Failed to delete snapshots matching {0}", f.Criteria));
        Command<DeleteMessagesSuccess>(_ => { });
        Command<DeleteMessagesFailure>(f =>
            _log.Warning(f.Cause, "Failed to delete events up to SeqNr {0}", f.ToSequenceNr));

        // Last: a message of any other type is a command only if the definition says how (Accepts).
        Command<object>(message => HandleForeign(message, Sender));
    }

    /// <summary>
    /// Decides on a command. <paramref name="origin"/> is who <c>Reply</c> answers: the command's sender, or,
    /// for a command produced by an effect, the sender of the command that issued that effect.
    /// </summary>
    /// <param name="command">The command.</param>
    /// <param name="origin">Who <c>Reply</c> answers.</param>
    /// <param name="reminder">The reminder delivery that carried the command, acknowledged once it is decided.</param>
    /// <param name="first">
    /// An effect of the library's own that runs before the decision's effects, whatever the decision is.
    /// </param>
    /// <param name="settlement">
    /// For the answer or the give-up of a recorded effect: the record that settles it, written in the same write as the
    /// decision's events, whatever the decision is.
    /// </param>
    private void HandleCommand(
        TCommand command, IActorRef origin, ReminderEnvelope? reminder, EntityEffect? first = null,
        Settlement? settlement = null)
    {
        var decision = _definition.Decide(State, command, NewContext());
        if (first is not null || settlement is not null)
        {
            ImmutableArray<object> records = settlement is null ? [] : [settlement.Record];
            if (decision is Handled<TCommand, TEvent> handled)
            {
                if (first is { } prefix)
                    handled = handled with
                    {
                        Effects = handled.Effects.IsDefault ? [prefix] : handled.Effects.Insert(0, prefix),
                    };
                Commit(handled, origin, reminder, records, settlement?.Then);
                return;
            }

            // The decision runs nothing, but the library's effect must still run, and the settlement must still be
            // journaled. A stash keeps the current message, so it happens here, before the commit.
            if (decision is Stashed)
            {
                if (settlement is null)
                {
                    Stash.Stash();
                }
                else
                {
                    // The current message may not be this command (after recovery it is not). The command is decided
                    // again when the stash is released, and the settlement is journaled then.
                    Stash.Prepend([new Envelope(new DecideAgain(settlement.Sequence, command!, settlement.Record, origin), Self)]);
                    records = [];
                }
            }
            else if (decision is NotHandled) Unhandled(command);
            else if (decision is not Ignored)
                throw new InvalidOperationException(
                    $"{EntityType.Value}.Decide returned an uninitialized Decision for command {command}.");

            var none = Decisions.None<TCommand, TEvent>();
            Commit(first is { } only ? none.Add(only) : none, origin, reminder, records, settlement?.Then);
            return;
        }

        switch (decision)
        {
            case Handled<TCommand, TEvent> handled:
                Commit(handled, origin, reminder);
                break;

            case Ignored:
                // Deliberately ignored reminders are acknowledged so they aren't redelivered.
                if (reminder is not null) AckReminder(reminder);
                StopIfRequested();
                break;

            case NotHandled:
                // Policy: an unhandled reminder is NACKed (scheduler retry policy applies)
                // rather than silently acknowledged.
                if (reminder is not null)
                {
                    NackReminder(reminder, "Entity decider returned NotHandled for reminder payload");
                    Unhandled(reminder);
                }
                else
                {
                    Unhandled(command);
                }
                StopIfRequested();
                break;

            case Stashed:
                // Stashes the original envelope + sender. A stashed reminder is not
                // acknowledged until it is actually processed after UnstashAll.
                Stash.Stash();
                StopIfRequested();
                break;

            default:
                throw new InvalidOperationException(
                    $"{EntityType.Value}.Decide returned an uninitialized Decision for command {command}.");
        }
    }

    private void HandleReminder(ReminderEnvelope envelope)
    {
        if (envelope.Message is PendingStart pending)
        {
            ReissueStart(pending, envelope);
            return;
        }

        // Handled.Schedule only accepts TCommand, so the payload was a TCommand when it was scheduled.
        // The check still guards against a payload that changed type in reminder storage.
        if (envelope.Message is TCommand command)
        {
            HandleCommand(command, Sender, envelope);
            return;
        }

        _log.Warning("Reminder {0} payload of type {1} is not a {2}",
            envelope.Key, envelope.Message?.GetType(), typeof(TCommand));
        NackReminder(envelope, $"Reminder payload is not a {typeof(TCommand).Name}");
        Unhandled(envelope);
    }

    /// <summary>A message that is not a <typeparamref name="TCommand"/>: typically another entity's reply.</summary>
    private void HandleForeign(object message, IActorRef origin)
    {
        if (message is TCommand command)
        {
            HandleCommand(command, origin, reminder: null);
            return;
        }

        if (_definition.TryAccept(message, out var accepted))
        {
            HandleCommand(accepted, origin, reminder: null);
            return;
        }

        // The sender gets no reply. Akka.NET reports an unhandled message at info level at most, and only
        // the first few of them, so say here what was refused and how to accept it.
        var type = message.GetType().Name;
        _log.Warning(
            "[{0}/{1}] received a message of type {2}, which its definition does not accept. " +
            "Declare the type with Accepts<{2}>, or send one of its commands.",
            EntityType.Value, _entityId.Value, type);
        Unhandled(message);
    }

    private void HandleEffectCompleted(EffectCompleted completed)
    {
        // An outcome sent just before a restart can reach the next incarnation, which never counted it.
        _executesInFlight = Math.Max(0, _executesInFlight - 1);

        if (completed.Sequence > 0)
        {
            SettleRecorded(completed);
            return;
        }

        // A start that the workflow answered, or whose give-up the entity decides on, needs its reminder no more.
        EntityEffect? first = null;
        if (completed.Effect is StartWorkflow start && completed.Failure is not UndeclaredWorkflowException)
        {
            var key = PendingStart.KeyOf(start.Workflow, start.Id);
            var stillInFlight = Settle(key);
            if (!stillInFlight && (completed.Failure is null || completed.Failed is not null))
            {
                first = new CancelTimerEffect(key);
                _startsSettled.Add(key);
            }
        }

        var outcome = completed.Outcome;
        if (completed.Failure is { } failure)
        {
            if (completed.Failed is null)
            {
                if (completed.Effect is StartWorkflow unanswered && failure is not UndeclaredWorkflowException)
                {
                    // The reminder that keeps the start is still stored, and issues it again.
                    _log.Error(failure, "Start of workflow [{0}/{1}] by entity [{2}/{3}] got no answer. It is issued " +
                                        "again within {4}.",
                        unanswered.Workflow.Value, unanswered.Id.Value, EntityType.Value, _entityId.Value,
                        StartRetryInterval);
                    StopIfRequested();
                    return;
                }

                // The state already records that the effect was issued. Resume re-derives it on the next
                // incarnation and a retry arm in Decide can re-issue it sooner. A handler that has to report a
                // failure to the entity returns it as an outcome instead of throwing.
                LogFailedEffect(completed.Effect, failure, "State is unchanged.");
                StopIfRequested();
                return;
            }

            // The effect says what a failure means to this entity: it is an outcome like any other.
            LogFailedEffect(completed.Effect, failure, "The entity decides on the failure.");
            outcome = completed.Failed(failure);
        }

        switch (outcome)
        {
            case TCommand command:
                HandleCommand(command, completed.Origin, reminder: null, first);
                break;

            case null when first is { } cancel:
                Commit(Decisions.None<TCommand, TEvent>().Add(cancel), completed.Origin, reminder: null);
                break;

            case null:
                _log.Debug("Effect {0} completed with no outcome", completed.Effect);
                StopIfRequested();
                break;

            default:
                throw new InvalidOperationException(
                    $"The handler for effect {completed.Effect} returned a {outcome.GetType()}, which is " +
                    $"not a {typeof(TCommand)}. A handler returns one of the entity's commands.");
        }
    }

    /// <summary>
    /// The one attempt of a recorded effect finished. Its answer is decided with an <see cref="EffectAcknowledged"/>
    /// in the same write; a failure, a time-out or an empty answer becomes the effect's give-up command, decided with
    /// an <see cref="EffectGaveUp"/>. A handler with no answer is acknowledged in a write of its own.
    /// </summary>
    private void SettleRecorded(EffectCompleted completed)
    {
        var sequence = completed.Sequence;
        var effect = completed.Effect;
        if (!_pending.ContainsKey(sequence))
        {
            _log.Debug("Dropping the late answer of effect {0} (id {1}) of entity [{2}/{3}]: it is already settled.",
                effect.GetType().Name, sequence, EntityType.Value, _entityId.Value);
            StopIfRequested();
            return;
        }

        var failure = completed.Failure;
        var withAnswer = _routes.TryGet(effect.GetType(), out var route) && route.OutcomeType is not null;
        TCommand answer = default!;
        if (failure is null && withAnswer)
        {
            if (completed.Outcome is TCommand command && command is not IUnion { Value: null })
                answer = command;
            else
                failure = new InvalidOperationException(completed.Outcome is null or IUnion { Value: null }
                    ? $"The handler {route.HandlerType.Name} answered with nothing."
                    : $"The handler {route.HandlerType.Name} answered with a {completed.Outcome.GetType().Name}, " +
                      $"which is not a {typeof(TCommand).Name}.");
        }

        if (failure is null)
        {
            var acknowledged = new EffectAcknowledged(sequence);
            if (withAnswer)
                HandleCommand(answer, completed.Origin, reminder: null, settlement: new Settlement(sequence, acknowledged, Then: null));
            else
                Commit(Decisions.None<TCommand, TEvent>(), completed.Origin, reminder: null, [acknowledged]);
            return;
        }

        var reason = failure is TimeoutException ? GaveUpReason.TimedOut : GaveUpReason.Failed;
        _log.Warning(failure, "Effect {0} (id {1}) of entity [{2}/{3}] gave up as {4}. Decide gets its give-up command.",
            effect.GetType().Name, sequence, EntityType.Value, _entityId.Value, reason);

        // Execute only accepts an IEffect<TCommand>, so the effect says what its give-up means to this entity.
        var gaveUp = ((IEffect<TCommand>)effect).GaveUp(new WorkGaveUp(effect, reason, failure.Message));
        HandleCommand(gaveUp, completed.Origin, reminder: null,
            settlement: new Settlement(sequence, new EffectGaveUp(sequence, 1, reason), Then: null));
    }

    /// <summary>The answer or give-up of a recorded effect was stashed by <c>Decide</c>, and the stash was released.</summary>
    private void HandleDecideAgain(DecideAgain again)
    {
        if (!_pending.ContainsKey(again.Sequence))
        {
            StopIfRequested();
            return;
        }

        HandleCommand((TCommand)again.Command, again.Origin, reminder: null,
            settlement: new Settlement(again.Sequence, again.Record, Then: null));
    }

    private void LogFailedEffect(object effect, Exception failure, string consequence)
    {
        if (failure is MissingEffectHandlerException missing)
            LogMissingHandler(missing);
        else if (failure is UndeclaredWorkflowException && effect is IWorkflowCall call)
            // The call's contents may hold personal data, so only the workflow type and id are named.
            _log.Error("Entity [{0}/{1}] called workflow [{2}/{3}], but its definition does not name workflow type " +
                       "[{2}] with Uses. The call was not sent. Add .Uses(...) with that workflow's definition to the " +
                       "entity definition.",
                EntityType.Value, _entityId.Value, call.Workflow.Value, call.Id.Value);
        else
            _log.Warning(failure, "Effect {0} failed for entity [{1}/{2}]. {3}",
                effect, EntityType.Value, _entityId.Value, consequence);
    }

    // The effect's contents may hold personal data, so only its type is named.
    private void LogMissingHandler(MissingEffectHandlerException missing) =>
        _log.Error("No effect handler is registered for effect {0} issued by [{1}/{2}]. Register it at startup with " +
                   "services.{3}. The effect was not run.",
            missing.EffectType, EntityType.Value, _entityId.Value, missing.HandlerInterface);

    /// <summary>
    /// Journals a decision and then runs its effects. The decision's events, <paramref name="records"/> and one
    /// <see cref="EffectIssued"/> for each recorded <c>Execute</c> go in one <c>PersistAll</c>, which is one atomic
    /// write: all of them are stored, or none.
    /// </summary>
    /// <param name="handled">The decision.</param>
    /// <param name="origin">Who <c>Reply</c> answers.</param>
    /// <param name="reminder">The reminder delivery that carried the command, acknowledged once it is decided.</param>
    /// <param name="records">Library records that settle effects: an acknowledgement or a give-up.</param>
    /// <param name="then">Runs last, after the effects. Used to decide the next interrupted effect after recovery.</param>
    private void Commit(
        Handled<TCommand, TEvent> handled, IActorRef origin, ReminderEnvelope? reminder,
        ImmutableArray<object> records = default, Action? then = null)
    {
        ImmutableArray<TEvent> events = handled.Events.IsDefault ? [] : handled.Events;
        ImmutableArray<EntityEffect> effects = handled.Effects.IsDefault ? [] : handled.Effects;

        // An effect type with no route is refused before the write: a recorded effect must be able to find its handler,
        // also after a restart. A configuration error, so it goes to supervision.
        var issued = new List<IssuedEffect>();
        foreach (var effect in effects)
        {
            if (effect is not ExecuteEffect { Recorded: true } execute) continue;
            if (!_routes.TryGet(execute.Effect.GetType(), out _))
            {
                var missing = new MissingEffectRouteException(execute.Effect.GetType(), typeof(TCommand));
                _log.Error("Entity [{0}/{1}] issued the effect {2}, and no handler is registered for it. Register it at " +
                           "startup with services.{3}. The effect was not journaled and not run.",
                    EntityType.Value, _entityId.Value, missing.EffectType, missing.Registration);
                throw missing;
            }

            issued.Add(new IssuedEffect(execute));
        }

        var batch = ImmutableArray.CreateBuilder<object>(events.Length + (records.IsDefault ? 0 : records.Length) + issued.Count);
        foreach (var @event in events) batch.Add(@event!);
        if (!records.IsDefault) batch.AddRange(records);
        var issuedAt = _timeProvider.Now;
        foreach (var effect in issued) batch.Add(new EffectIssued(effect.Execute.Effect, issuedAt));

        // The handler runs once for each entry, in batch order, so the n-th EffectIssued belongs to the n-th recorded
        // Execute. Matched by order, not by reference: a journal may hand back a copy of the entry.
        var next = 0;

        // An empty batch is fine here: PersistAll routes onCompleteAsync through the internal
        // *stashing* Defer, so no further commands are processed until the effects are dispatched.
        // (Public DeferAsync does NOT stash - don't swap it in.)
        PersistAll(batch.ToImmutable(), entry =>
        {
            switch (entry)
            {
                case EffectIssued record:
                    // Akka.Persistence sets LastSequenceNr to this entry's number before it calls the handler.
                    var sequence = LastSequenceNr;
                    issued[next++].Sequence = sequence;
                    _pending[sequence] = new PendingEffect(sequence, record.Effect, record.IssuedAt);
                    break;
                case EffectAcknowledged acknowledged:
                    _pending.Remove(acknowledged.Id);
                    break;
                case EffectGaveUp gaveUp:
                    _pending.Remove(gaveUp.Id);
                    break;
                default:
                    Apply((TEvent)entry);
                    break;
            }
        }, onCompleteAsync: async () =>
        {
            if (!events.IsEmpty || !records.IsDefaultOrEmpty || issued.Count > 0) MaybeSnapshot();
            await ExecuteEffectsAsync(effects, origin, issued);
            if (reminder is not null) AckReminder(reminder);

            // The decision is complete. It may have started new Execute effects; then the entity keeps running.
            StopIfRequested();
            then?.Invoke();
        });
    }

    /// <summary>
    /// The host asks the entity to stop. With an <c>Execute</c> handler in flight the entity stops later, once
    /// it has decided on every outcome. The host buffers the entity's messages until then.
    /// </summary>
    /// <remarks>
    /// The wait is bounded during a rebalance or a node shutdown: the shard stops the entity itself once its
    /// hand-off timeout has passed.
    /// </remarks>
    private void RequestStop()
    {
        _stopRequested = true;
        if (_executesInFlight > 0)
            _log.Debug("Stop deferred until {0} Execute effects in flight have completed", _executesInFlight);
        StopIfRequested();
    }

    /// <summary>Stops the entity if its host asked it to and no <c>Execute</c> handler is in flight.</summary>
    private void StopIfRequested()
    {
        if (_stopRequested && _executesInFlight == 0) Context.Stop(Self);
    }

    #endregion

    #region Effects

    /// <param name="effects">The decision's effects, in order.</param>
    /// <param name="origin">Who <c>Reply</c> answers.</param>
    /// <param name="issued">The decision's recorded <c>Execute</c> effects, in order, with their ids.</param>
    private async Task ExecuteEffectsAsync(
        ImmutableArray<EntityEffect> effects, IActorRef origin, IReadOnlyList<IssuedEffect> issued)
    {
        var recorded = 0;
        foreach (var effect in effects)
        {
            switch (effect)
            {
                case ExecuteEffect { Recorded: true }:
                    // Started at its place among the effects, not awaited: a later Reply leaves at once.
                    StartRecorded(issued[recorded++], origin);
                    break;

                case ReplyEffect reply:
                    origin.Tell(reply.Response, Self);
                    break;

                case TellEffect tell:
                    tell.Recipient.Tell(tell.Message, Self);
                    break;

                case ExecuteEffect { Effect: IWorkflowCall call } execute when !_definition.Declares(call):
                    // Not a reason to restart: the call fails, and the entity gets its give-up if it asked for one.
                    _executesInFlight++;
                    Self.Tell(new EffectCompleted(execute.Effect, null,
                        new UndeclaredWorkflowException(EntityType, call.Workflow), origin, execute.Failed), Self);
                    break;

                case ExecuteEffect { Effect: StartWorkflow start } execute:
                    // The reminder is stored before the start is sent, so a start lost with the process is issued
                    // again. If the reminder cannot be stored, the start is not sent.
                    var key = PendingStart.KeyOf(start.Workflow, start.Id);
                    await StorePendingStartAsync(key, new PendingStart(start.Workflow.Value, start.Id.Value, start.Input));
                    _startsInFlight[key] = _startsInFlight.GetValueOrDefault(key) + 1;
                    _startsSettled.Remove(key);
                    await StartExecuteAsync(execute, origin);
                    break;

                case ExecuteEffect execute:
                    // Started, not awaited: the outcome returns as an EffectCompleted message.
                    await StartExecuteAsync(execute, origin);
                    break;

                case SendEffect send:
                    StartSend(send, origin);
                    break;

                case RunEffect run:
                    // Awaited in place; later effects and commands wait. Keep this await on the actor
                    // scheduler so the rest of the decision stays inside the actor.
                    var outcome = await InvokeAsync(_services.CreateAsyncScope(),
                        async (services, ct) =>
                        {
                            await run.ExecuteAsync(services, ct);
                            return null;
                        }, run.Timeout);
                    _lifetime.Token.ThrowIfCancellationRequested();
                    if (outcome.Failure is { } failure)
                        _log.Warning(failure, "Run effect failed for entity [{0}/{1}]. Continuing with the next effect.",
                            EntityType.Value, _entityId.Value);
                    break;

                case ScheduleEffect schedule:
                    EnsureScheduled(await Reminders.ScheduleSingleReminderAsync(
                        new ReminderKey(schedule.Key),
                        _timeProvider.Now + schedule.Delay,
                        schedule.Message,
                        ct: _lifetime.Token));
                    break;

                case ScheduleRecurringEffect recurring:
                    EnsureScheduled(await Reminders.ScheduleRecurringReminderAsync(
                        new ReminderKey(recurring.Key),
                        _timeProvider.Now + recurring.InitialDelay,
                        recurring.Interval,
                        recurring.Message,
                        ct: _lifetime.Token));
                    break;

                case CancelTimerEffect cancel:
                    var cancelled = await Reminders.CancelReminderAsync(
                        new ReminderKey(cancel.Key), _lifetime.Token);
                    // Cancelling a reminder that doesn't exist is not an error.
                    if (cancelled.ResponseCode is not (ReminderCancelResponseCode.Success
                        or ReminderCancelResponseCode.NotFound))
                    {
                        if (!cancel.BestEffort)
                            throw new InvalidOperationException(
                                $"Failed to cancel reminder [{cancel.Key}]: {cancelled.ResponseCode} {cancelled.Message}");

                        _log.Warning("Failed to cancel reminder [{0}] for entity [{1}/{2}]: {3} {4}. Continuing.",
                            cancel.Key, EntityType.Value, _entityId.Value, cancelled.ResponseCode, cancelled.Message);
                    }

                    break;

                case UnstashAllEffect:
                    Stash.UnstashAll();
                    break;

                case StopEffect:
                    Passivate();
                    return; // Handled.Add guarantees Stop is the last effect

                default:
                    throw new InvalidOperationException($"Unsupported entity effect: {effect.Value}");
            }
        }
    }

    private async Task StartExecuteAsync(ExecuteEffect execute, IActorRef origin)
    {
        if (execute.Resolve is null)
            throw new InvalidOperationException("An ExecuteEffect must be created through Handled.Execute.");

        var scope = _services.CreateAsyncScope();
        Func<CancellationToken, Task<object?>> invoke;
        try
        {
            // A missing or ambiguous handler is a configuration error and goes to supervision.
            // Only the handler's own run is best effort.
            invoke = execute.Resolve(scope.ServiceProvider);
        }
        catch (Exception ex)
        {
            if (ex is MissingEffectHandlerException missing)
                LogMissingHandler(missing);
            await scope.DisposeAsync();
            throw;
        }

        StartHandler(scope, invoke, execute.Effect, origin, execute.Failed, sequence: 0);
    }

    /// <summary>
    /// Runs the one attempt of a recorded effect through its route. An effect whose record was not journaled, because
    /// the journal rejected the write, is not run.
    /// </summary>
    private void StartRecorded(IssuedEffect issued, IActorRef origin)
    {
        var effect = issued.Execute.Effect;
        if (issued.Sequence == 0)
        {
            _log.Error("The effect {0} of entity [{1}/{2}] was not journaled, so it is not run.",
                effect.GetType().Name, EntityType.Value, _entityId.Value);
            return;
        }

        if (!_routes.TryGet(effect.GetType(), out var route))
            throw new MissingEffectRouteException(effect.GetType(), typeof(TCommand));

        var delivery = new EffectDelivery(
            new EffectId($"{EntityType.Value}/{_entityId.Value}/{issued.Sequence}", issued.Sequence), Attempt: 1);
        var scope = _services.CreateAsyncScope();
        var services = scope.ServiceProvider;
        StartHandler(scope, ct => route.Invoke(services, effect, delivery, ct), effect, origin, failed: null, issued.Sequence);
    }

    /// <summary>
    /// Starts a handler and does not wait for it: its outcome comes back as an <see cref="EffectCompleted"/>.
    /// </summary>
    /// <param name="sequence">The id of a recorded effect, or 0.</param>
    private void StartHandler(
        AsyncServiceScope scope, Func<CancellationToken, Task<object?>> invoke, object effect, IActorRef origin,
        Func<Exception, object?>? failed, long sequence)
    {
        var self = Self;
        var lifetime = _lifetime.Token;
        var log = _log;
        _executesInFlight++;
        _ = InvokeAsync(scope, (_, ct) => invoke(ct), timeout: null).ContinueWith(t =>
        {
            // This incarnation is gone. Its successor gives a recorded effect up as interrupted.
            if (lifetime.IsCancellationRequested) return;

            if (t.IsCompletedSuccessfully)
            {
                self.Tell(new EffectCompleted(effect, t.Result.Result, t.Result.Failure, origin, failed, sequence), self);
            }
            else
            {
                log.Error(t.Exception, "The outcome of effect {0} could not be observed", effect.GetType().Name);
                // Still report back, so that the entity no longer counts the effect as in flight. A recorded effect
                // gives up.
                self.Tell(new EffectCompleted(effect, null, sequence > 0 ? t.Exception : null, origin, null, sequence), self);
            }
        }, TaskScheduler.Default);
    }

    private void StartSend(SendEffect send, IActorRef origin)
    {
        if (send.ResolveRegion is null)
            throw new InvalidOperationException("A SendEffect must be created through Handled.Send.");

        IActorRef region;
        try
        {
            region = send.ResolveRegion(Context.System);
        }
        catch (MissingActorRegistryEntryException error)
        {
            throw new InvalidOperationException(
                $"No durable entity type with command protocol {send.CommandType} is hosted in this ActorSystem, so " +
                $"entity [{EntityType.Value}/{_entityId.Value}] cannot send to [{send.Target.Value}]. For a union " +
                $"protocol, name it explicitly: Send<{send.CommandType.Name}Command>(...).", error);
        }

        var self = Self;
        var lifetime = _lifetime.Token;
        var log = _log;
        // Ask gives the target a reply address that lives until it answers, and ties the answer to the origin
        // of the command that issued the send. The entity's own reminders are its deadline, not this call.
        _ = region.Ask(new ShardingEnvelope(send.Target.Value, send.Command), timeout: null, cancellationToken: lifetime)
            .ContinueWith(t =>
            {
                if (lifetime.IsCancellationRequested) return;

                if (t.IsCompletedSuccessfully)
                    self.Tell(new ForeignReply(t.Result, origin), self);
                else
                    log.Warning(t.Exception, "Send of {0} to [{1}] got no reply", send.Command, send.Target.Value);
            }, TaskScheduler.Default);
    }

    #region Pending starts

    /// <summary>
    /// How long the reminder of a pending start waits before it issues the start again: the time limit of one
    /// attempt. An attempt in flight has its answer or its time-out by then.
    /// </summary>
    private TimeSpan StartRetryInterval => _options.EffectTimeout;

    private async Task StorePendingStartAsync(string key, PendingStart pending) =>
        EnsureScheduled(await Reminders.ScheduleSingleReminderAsync(
            new ReminderKey(key), _timeProvider.Now + StartRetryInterval, pending, ct: _lifetime.Token));

    /// <summary>Counts an answer to a start. True when another start with the same key is still in flight.</summary>
    private bool Settle(string key)
    {
        if (!_startsInFlight.TryGetValue(key, out var count)) return false;
        if (count > 1)
        {
            _startsInFlight[key] = count - 1;
            return true;
        }

        _startsInFlight.Remove(key);
        return false;
    }

    /// <summary>
    /// The reminder of a start that has not been answered fired. It issues the start again, unless this incarnation
    /// still waits for an answer: then it only waits another interval. The delivery is acknowledged once the reminder
    /// for the next attempt is stored.
    /// </summary>
    private void ReissueStart(PendingStart pending, ReminderEnvelope envelope)
    {
        var none = Decisions.None<TCommand, TEvent>();
        var key = PendingStart.KeyOf(new WorkflowTypeName(pending.Workflow), new WorkflowId(pending.Id));

        if (_startsInFlight.ContainsKey(key))
        {
            Commit(none.Add(new ScheduleEffect(key, StartRetryInterval, pending)), Context.System.DeadLetters, envelope);
            return;
        }

        if (_startsSettled.Contains(key))
        {
            // Delivered while the answer or the give-up was removing the reminder. The start is settled.
            _log.Debug("Dropping a stale delivery of the pending start of workflow [{0}/{1}]", pending.Workflow, pending.Id);
            Commit(none.Add(new CancelTimerEffect(key)), Context.System.DeadLetters, envelope);
            return;
        }

        if (!_definition.Used.TryGetValue(new WorkflowTypeName(pending.Workflow), out var use)
            || use.Restart(new WorkflowId(pending.Id), pending.Input) is not { } start)
        {
            // The definition changed since the start was stored. A repeat cannot succeed, so the reminder is removed.
            _log.Error("Entity [{0}/{1}] has a pending start of workflow [{2}/{3}], but its definition does not use " +
                       "workflow type [{2}] with that input type. The start is dropped.",
                EntityType.Value, _entityId.Value, pending.Workflow, pending.Id);
            Commit(none.Add(new CancelTimerEffect(key)), Context.System.DeadLetters, envelope);
            return;
        }

        _log.Info("Issuing the start of workflow [{0}/{1}] again: it has not been answered.", pending.Workflow, pending.Id);
        Commit(none.Add(start), Context.System.DeadLetters, envelope);
    }

    #endregion

    private readonly record struct EffectOutcome(object? Result, Exception? Failure);

    /// <summary>
    /// Runs one piece of service work inside <paramref name="scope"/> under a time budget. Never throws for the
    /// work's own failure or timeout; those come back in the outcome. Disposes the scope once the work has
    /// really finished, which can be after a timeout.
    /// </summary>
    private async Task<EffectOutcome> InvokeAsync(
        AsyncServiceScope scope,
        Func<IServiceProvider, CancellationToken, Task<object?>> invoke,
        TimeSpan? timeout)
    {
        var limit = timeout ?? _options.EffectTimeout;
        var cts = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        cts.CancelAfter(limit);

        Task<object?> task;
        try
        {
            // Service awaits must remain able to finish after this actor stops. Task.Run keeps
            // them off the actor synchronization context; only interpreting the result returns to it.
            task = Task.Run(() =>
            {
                cts.Token.ThrowIfCancellationRequested();
                return invoke(scope.ServiceProvider, cts.Token);
            });
        }
        catch
        {
            cts.Dispose();
            await scope.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        try
        {
            return new EffectOutcome(await task.WaitAsync(limit, _lifetime.Token).ConfigureAwait(false), null);
        }
        catch (Exception ex) when (!_lifetime.IsCancellationRequested)
        {
            // Either WaitAsync gave up first, or our CancelAfter fired and the task honoured it.
            var timedOut = (ex is TimeoutException && !task.IsCompleted) || cts.IsCancellationRequested;
            var failure = timedOut
                ? new TimeoutException($"Effect did not complete within {limit}.", ex)
                : ex;

            return new EffectOutcome(null, failure);
        }
        finally
        {
            // A timed-out task may still be using scoped services; never dispose
            // its scope underneath it. Release once it actually finishes.
            if (task.IsCompleted)
            {
                cts.Dispose();
                await scope.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                ReleaseWhenComplete(task, scope, cts, _log);
            }
        }
    }

    private static void ReleaseWhenComplete(
        Task task, AsyncServiceScope scope, CancellationTokenSource cts, ILoggingAdapter log)
    {
        // TaskScheduler.Default: this may run after the actor that started it has stopped.
        _ = task.ContinueWith(async t =>
        {
            try
            {
                if (t.IsFaulted)
                    log.Warning(t.Exception, "Effect failed after it had already timed out");
                cts.Dispose();
                await scope.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to dispose the DI scope of a timed-out effect");
            }
        }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default).Unwrap();
    }

    private static void EnsureScheduled(ReminderProtocol.ReminderScheduled result)
    {
        if (result.ResponseCode != ReminderScheduleResponseCode.Success)
            throw new InvalidOperationException(
                $"Failed to schedule reminder [{result.Key}]: {result.ResponseCode} {result.Message}");
    }

    /// <summary>
    /// Uses the sharding passivation protocol in both hosting modes: the parent (shard or
    /// local entity host) stops buffering for this entity and sends <see cref="StopEntity"/> back.
    /// </summary>
    /// <remarks>
    /// Sender must be explicit: this runs inside an async persistence callback where the
    /// implicit sender isn't reliably Self, and the shard identifies the entity by Sender.
    /// </remarks>
    private void Passivate() => Context.Parent.Tell(new Passivate(StopEntity.Instance), Self);

    #endregion

    #region Reminder acknowledgement

    // Ack/Nack are fire-and-observe: a failed ack only risks a duplicate delivery, which
    // deciders must already tolerate, so it shouldn't block the entity's mailbox.
    private void AckReminder(ReminderEnvelope envelope)
    {
        var log = _log;
        _ = Reminders.AckAsync(envelope).ContinueWith(t =>
        {
            if (t.IsFaulted)
                log.Warning(t.Exception, "Failed to ack reminder {0}; it may be redelivered", envelope.Key);
            else if (t.Result.ResponseCode == ReminderAckResponseCode.NotFound)
                log.Debug("Ack for superseded or removed reminder {0} returned NotFound: {1}", envelope.Key, t.Result.Message);
            else if (t.Result.ResponseCode != ReminderAckResponseCode.Success)
                log.Warning("Ack for reminder {0} returned {1}: {2}", envelope.Key, t.Result.ResponseCode, t.Result.Message);
        }, TaskScheduler.Default);
    }

    private void NackReminder(ReminderEnvelope envelope, string reason)
    {
        var log = _log;
        _ = Reminders.NackAsync(envelope, reason).ContinueWith(t =>
        {
            if (t.IsFaulted)
                log.Warning(t.Exception, "Failed to nack reminder {0}", envelope.Key);
        }, TaskScheduler.Default);
    }

    #endregion

    #region Snapshots

    private void MaybeSnapshot()
    {
        if (_options.SnapshotEvery <= 0) return;
        if (LastSequenceNr - _lastSnapshotSequenceNr < _options.SnapshotEvery) return;

        _lastSnapshotSequenceNr = LastSequenceNr;
        // The pending effects go with the state: with events deleted on snapshot, their records can be gone from the journal.
        SaveSnapshot(new EntitySnapshot(State!, [.. _pending.Values.Cast<object>()]));
    }

    private void HandleSnapshotSaved(SaveSnapshotSuccess success)
    {
        // Snapshots are taken roughly every SnapshotEvery events, so this keeps approximately
        // the newest KeepSnapshots of them. Every retained snapshot has SeqNr > cutoff, so
        // deleting events <= cutoff always preserves a recoverable path.
        var cutoff = success.Metadata.SequenceNr - (long)_options.SnapshotEvery * Math.Max(1, _options.KeepSnapshots);
        if (cutoff <= 0) return;

        DeleteSnapshots(new SnapshotSelectionCriteria(cutoff));
        if (_options.DeleteEventsOnSnapshot) DeleteMessages(cutoff);
    }

    #endregion

    protected override void PreRestart(Exception reason, object message)
    {
        // A deferred stop must outlive a restart: the host buffers this entity's messages until it stops.
        if (_stopRequested) Self.Tell(StopEntity.Instance, ActorRefs.NoSender);
        base.PreRestart(reason, message);
    }

    protected override void PostStop()
    {
        _lifetime.Cancel();
        base.PostStop();
    }

    /// <summary>The result of an <c>Execute</c> handler, sent by the entity to itself.</summary>
    /// <param name="Sequence">The id of a recorded effect, or 0 for an effect that is not recorded.</param>
    private sealed record EffectCompleted(
        object Effect, object? Outcome, Exception? Failure, IActorRef Origin, Func<Exception, object?>? Failed,
        long Sequence = 0)
        : INoSerializationVerificationNeeded;

    /// <summary>
    /// The answer or give-up of a recorded effect, which <c>Decide</c> stashed. Decided again when the stash is
    /// released; its settlement is journaled then.
    /// </summary>
    private sealed record DecideAgain(long Sequence, object Command, object Record, IActorRef Origin)
        : INoSerializationVerificationNeeded;

    /// <summary>The library record that settles a recorded effect, written with the decision on its answer or give-up.</summary>
    /// <param name="Sequence">The effect's id.</param>
    /// <param name="Record">An <see cref="EffectAcknowledged"/> or an <see cref="EffectGaveUp"/>.</param>
    /// <param name="Then">Runs once the decision's effects have run.</param>
    private sealed record Settlement(long Sequence, object Record, Action? Then);

    /// <summary>A recorded <c>Execute</c> of a decision, and the id its record got in the write.</summary>
    private sealed class IssuedEffect(ExecuteEffect execute)
    {
        public ExecuteEffect Execute { get; } = execute;

        /// <summary>The sequence number of its <see cref="EffectIssued"/>. 0 until the write succeeds.</summary>
        public long Sequence { get; set; }
    }

    /// <summary>Another entity's reply to a <c>Send</c>, with the origin of the command that sent it.</summary>
    private sealed record ForeignReply(object Message, IActorRef Origin) : INoSerializationVerificationNeeded;
}
