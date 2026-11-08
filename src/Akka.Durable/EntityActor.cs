using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Event;
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

    // Cancelled when this incarnation stops so in-flight Run effects are asked to stop.
    private readonly CancellationTokenSource _lifetime = new();

    // Created lazily so entities that never touch reminders don't require them to be configured.
    private IReminderClient? _reminderClient;

    private long _lastSnapshotSequenceNr;

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

    #region Recovery

    private void Recovers()
    {
        Recover<SnapshotOffer>(offer =>
        {
            if (offer.Snapshot is TState state)
            {
                State = state;
                _lastSnapshotSequenceNr = offer.Metadata.SequenceNr;
                _log.Debug("Recovered snapshot at SeqNr {0}", offer.Metadata.SequenceNr);
            }
            else
            {
                // Ignoring the snapshot is unsafe: Akka.Persistence resumes event replay AFTER the
                // snapshot's sequence number, so every event the snapshot covered would be silently lost.
                throw new EntityRecoveryException(
                    $"Entity [{PersistenceId}] cannot recover: snapshot at sequence number " +
                    $"[{offer.Metadata.SequenceNr}] is of type [{offer.Snapshot?.GetType()}] but the expected " +
                    $"state type is [{typeof(TState)}]. The snapshot cannot be skipped, because events it covers " +
                    "are not replayed. If the entity's events are still in the journal, delete the snapshot so " +
                    "the entity recovers from its events.");
            }
        });

        // Replay only domain events through Evolve. Decide and effects never run during recovery.
        // A C# 15 union TEvent comes back from the journal as the union, because WithDurableEntity requires
        // it to have its own serializer (a union marked [AkkaSerializable(Manifest = ...)]).
        Recover<TEvent>(Apply);

        Recover<RecoveryCompleted>(_ =>
            _log.Debug("Recovery completed at SeqNr {0}", LastSequenceNr));

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

    #endregion

    #region Command processing

    private void Commands()
    {
        // Must precede Command<TCommand> so reminder envelopes are never treated as commands.
        Command<ReminderEnvelope>(HandleReminder);

        // State query, handled outside TCommand so Decide never sees it. Akka.Persistence stashes
        // incoming messages while events persist and effects run, so a query sent after a command is
        // answered only once that command's events are committed and its effects have finished.
        // Must precede Command<TCommand> for the same reason as ReminderEnvelope.
        Command<GetEntityState>(_ => Sender.Tell(new EntityStateReply(State!, LastSequenceNr)));

        Command<TCommand>(command => HandleCommand(command, reminder: null));
        Command<StopEntity>(_ => Context.Stop(Self));

        Command<SaveSnapshotSuccess>(HandleSnapshotSaved);
        Command<SaveSnapshotFailure>(f =>
            _log.Warning(f.Cause, "Failed to save snapshot at SeqNr {0}", f.Metadata.SequenceNr));
        Command<DeleteSnapshotsSuccess>(_ => { });
        Command<DeleteSnapshotsFailure>(f =>
            _log.Warning(f.Cause, "Failed to delete snapshots matching {0}", f.Criteria));
        Command<DeleteMessagesSuccess>(_ => { });
        Command<DeleteMessagesFailure>(f =>
            _log.Warning(f.Cause, "Failed to delete events up to SeqNr {0}", f.ToSequenceNr));
    }

    private void HandleCommand(TCommand command, ReminderEnvelope? reminder)
    {
        var sender = Sender; // captured per-command; Reply targets this after async completion
        var context = new DecisionContext<TState, TCommand, TEvent>(_entityId, EntityType, _timeProvider.Now, State);

        switch (_definition.Decide(State, command, context))
        {
            case Handled<TCommand, TEvent> handled:
                Commit(handled, sender, reminder);
                break;

            case Ignored:
                // Deliberately ignored reminders are acknowledged so they aren't redelivered.
                if (reminder is not null) AckReminder(reminder);
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
                break;

            case Stashed:
                // Stashes the original envelope + sender. A stashed reminder is not
                // acknowledged until it is actually processed after UnstashAll.
                Stash.Stash();
                break;

            default:
                throw new InvalidOperationException(
                    $"{EntityType.Value}.Decide returned an uninitialized Decision for command {command}.");
        }
    }

    private void HandleReminder(ReminderEnvelope envelope)
    {
        // Handled.Schedule only accepts TCommand, so the payload was a TCommand when it was scheduled.
        // The check still guards against a payload that changed type in reminder storage.
        if (envelope.Message is TCommand command)
        {
            HandleCommand(command, envelope);
            return;
        }

        _log.Warning("Reminder {0} payload of type {1} is not a {2}",
            envelope.Key, envelope.Message?.GetType(), typeof(TCommand));
        NackReminder(envelope, $"Reminder payload is not a {typeof(TCommand).Name}");
        Unhandled(envelope);
    }

    private void Commit(Handled<TCommand, TEvent> handled, IActorRef sender, ReminderEnvelope? reminder,
        HashSet<object>? earlierStages = null)
    {
        var stages = earlierStages ?? [];
        if (!stages.Add(handled.Stage))
            throw new InvalidOperationException(Handled<TCommand, TEvent>.ReusedStageMessage);

        ImmutableArray<TEvent> events = handled.Events.IsDefault ? [] : handled.Events;
        ImmutableArray<EntityEffect<TCommand, TEvent>> effects = handled.Effects.IsDefault ? [] : handled.Effects;

        // Zero events is fine here: PersistAll routes onCompleteAsync through the internal
        // *stashing* Defer, so no further commands are processed until effects complete.
        // (Public DeferAsync does NOT stash - don't swap it in.)
        PersistAll(events, Apply, onCompleteAsync: async () =>
        {
            if (!events.IsEmpty) MaybeSnapshot();

            // A Run starts a new persistence stage. Its terminal branch owns the acknowledgement.
            if (await ExecuteEffectsAsync(effects, sender, reminder, stages) && reminder is not null)
                AckReminder(reminder);
        });
    }

    #endregion

    #region Effects

    private async Task<bool> ExecuteEffectsAsync(
        ImmutableArray<EntityEffect<TCommand, TEvent>> effects, IActorRef sender, ReminderEnvelope? reminder,
        HashSet<object> stages)
    {
        foreach (var effect in effects)
        {
            switch (effect)
            {
                case ReplyEffect reply:
                    sender.Tell(reply.Response, Self);
                    break;

                case TellEffect tell:
                    tell.Recipient.Tell(tell.Message, Self);
                    break;

                case RunEffect<TCommand, TEvent> run:
                    // Keep this await on the actor scheduler: reading State and committing the next
                    // decision must remain inside the actor, even though service work is independent.
                    var outcome = await ExecuteRunAsync(run);
                    _lifetime.Token.ThrowIfCancellationRequested();
                    // Only the invocation is inside ExecuteRunAsync's failure boundary. Exceptions in
                    // continuation construction, persistence, and later effects go to supervision.
                    Handled<TCommand, TEvent> next;
                    if (outcome.Failure is { } failure)
                    {
                        _log.Warning(failure, "Run effect failed for entity [{0}/{1}]. Executing failure continuation.",
                            EntityType.Value, _entityId.Value);
                        next = run.OnFailure(State!, _timeProvider.Now, failure);
                    }
                    else
                    {
                        next = run.OnSuccess(State!, _timeProvider.Now, outcome.Result);
                    }
                    // Register the next persist before releasing this callback. Do not await it here:
                    // its journal acknowledgements must be processed by the persistent actor.
                    Commit(next, sender, reminder, stages);
                    return false;

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
                        throw new InvalidOperationException(
                            $"Failed to cancel reminder [{cancel.Key}]: {cancelled.ResponseCode} {cancelled.Message}");
                    break;

                case UnstashAllEffect:
                    Stash.UnstashAll();
                    break;

                case StopEffect:
                    Passivate();
                    return true; // Handled.Add guarantees Stop is the last effect

                default:
                    throw new InvalidOperationException($"Unsupported entity effect: {effect.Value}");
            }
        }
        return true;
    }

    private sealed record RunOutcome(object? Result, Exception? Failure);

    private async Task<RunOutcome> ExecuteRunAsync(RunEffect<TCommand, TEvent> effect)
    {
        var timeout = effect.Timeout ?? _options.EffectTimeout;
        var scope = _services.CreateAsyncScope();
        var cts = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        cts.CancelAfter(timeout);

        Task<object?> task;
        try
        {
            // Service awaits must remain able to finish after this actor stops. Task.Run keeps
            // them off the actor synchronization context; only interpreting the result returns to it.
            task = Task.Run(() =>
            {
                cts.Token.ThrowIfCancellationRequested();
                return effect.ExecuteAsync(scope.ServiceProvider, cts.Token);
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
            return new RunOutcome(await task.WaitAsync(timeout, _lifetime.Token).ConfigureAwait(false), null);
        }
        catch (Exception ex) when (!_lifetime.IsCancellationRequested)
        {
            // Either WaitAsync gave up first, or our CancelAfter fired and the task honoured it.
            var timedOut = (ex is TimeoutException && !task.IsCompleted) || cts.IsCancellationRequested;
            var failure = timedOut
                ? new TimeoutException($"Run effect did not complete within {timeout}.", ex)
                : ex;

            return new RunOutcome(null, failure);
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
                    log.Warning(t.Exception, "Run effect failed after it had already timed out");
                cts.Dispose();
                await scope.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                log.Error(ex, "Failed to dispose DI scope of a timed-out Run effect");
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
        SaveSnapshot(State!);
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

    protected override void PostStop()
    {
        _lifetime.Cancel();
        base.PostStop();
    }

    /// <summary>
    /// Stop message handed back by the shard / local host after <see cref="Passivate"/>.
    /// </summary>
    /// <remarks>
    /// Only ever sent by an entity's own parent (its shard or local host) on the same node, also as the
    /// shard's hand-off stop message, so it never crosses a serialization boundary.
    /// </remarks>
    internal sealed class StopEntity : INoSerializationVerificationNeeded
    {
        public static readonly StopEntity Instance = new();
        private StopEntity() { }
    }
}
