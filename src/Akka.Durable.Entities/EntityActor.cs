using System.Collections.Immutable;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Abstractions.Entities;
using Akka.Event;
using Akka.Persistence;
using Akka.Reminders;
using Microsoft.Extensions.DependencyInjection;

namespace Akka.Durable.Entities;

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
                _log.Warning("Ignoring incompatible snapshot of type {0} at SeqNr {1}",
                    offer.Snapshot?.GetType(), offer.Metadata.SequenceNr);
            }
        });

        // Replay only domain events through Evolve. Decide and effects never run during recovery.
        // TODO: if TEvent is a C# 15 union, a journal that hands back the concrete case type
        // (rather than the boxed union struct) will not match here. See spec P0 serialization item.
        Recover<TEvent>(Apply);

        Recover<RecoveryCompleted>(_ =>
            _log.Debug("Recovery completed at SeqNr {0}", LastSequenceNr));
    }

    private void Apply(TEvent @event) => State = _definition.Evolve(State, @event);

    #endregion

    #region Command processing

    private void Commands()
    {
        // Must precede Command<TCommand> so reminder envelopes are never treated as commands.
        Command<ReminderEnvelope>(HandleReminder);
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
        var context = new DecisionContext<TCommand, TEvent>(_entityId, EntityType, _timeProvider.Now);

        switch (_definition.Decide(State, command, context))
        {
            case Handled<TCommand, TEvent> handled:
                Commit(handled, sender, reminder);
                break;

            case Ignored:
                // Deliberately ignored reminders are acknowledged so they aren't redelivered.
                if (reminder is not null) AckReminder(reminder);
                break;

            // Fully qualified: inside an actor, bare `Unhandled` binds to ActorBase.Unhandled(object).
            case global::Akka.Durable.Entities.Unhandled:
                // Policy: an unhandled reminder is NACKed (scheduler retry policy applies)
                // rather than silently acknowledged.
                if (reminder is not null)
                {
                    NackReminder(reminder, "Entity decider returned Unhandled for reminder payload");
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
        // Handled.Schedule only accepts TCommand, so the payload was a TCommand when it was
        // scheduled. This check still matters for the reminder-storage round trip:
        // TODO: same C# 15 union caveat as Recover<TEvent>: a serializer that hands back the
        // concrete case type (instead of the boxed union) will not match a union TCommand.
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

    private void Commit(Handled<TCommand, TEvent> handled, IActorRef sender, ReminderEnvelope? reminder)
    {
        ImmutableArray<TEvent> events = handled.Events.IsDefault ? [] : handled.Events;
        ImmutableArray<EntityEffect> effects = handled.Effects.IsDefault ? [] : handled.Effects;

        // Zero events is fine here: PersistAll routes onCompleteAsync through the internal
        // *stashing* Defer, so no further commands are processed until effects complete.
        // (Public DeferAsync does NOT stash - don't swap it in.)
        PersistAll(events, Apply, onCompleteAsync: async () =>
        {
            if (!events.IsEmpty) MaybeSnapshot();

            await ExecuteEffectsAsync(effects, sender);

            // Ack only after persistence and effects have succeeded.
            if (reminder is not null) AckReminder(reminder);
        });
    }

    #endregion

    #region Effects

    private async Task ExecuteEffectsAsync(ImmutableArray<EntityEffect> effects, IActorRef sender)
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

                case RunEffect run:
                    await ExecuteRunAsync(run, sender);
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
                        throw new InvalidOperationException(
                            $"Failed to cancel reminder [{cancel.Key}]: {cancelled.ResponseCode} {cancelled.Message}");
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

    private async Task ExecuteRunAsync(RunEffect effect, IActorRef sender)
    {
        var timeout = effect.Timeout ?? _options.EffectTimeout;
        var scope = _services.CreateAsyncScope();
        var cts = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        cts.CancelAfter(timeout);

        Task<object?> task;
        try
        {
            task = effect.ExecuteAsync(scope.ServiceProvider, cts.Token);
        }
        catch
        {
            cts.Dispose();
            await scope.DisposeAsync();
            throw;
        }

        object? completion;
        try
        {
            completion = await task.WaitAsync(timeout);
        }
        catch (Exception ex) when (!_lifetime.IsCancellationRequested)
        {
            // Either WaitAsync gave up first, or our CancelAfter fired and the task honoured it.
            var timedOut = (ex is TimeoutException && !task.IsCompleted) || cts.IsCancellationRequested;
            var failure = timedOut
                ? new TimeoutException($"Run effect did not complete within {timeout}.", ex)
                : ex;

            if (effect.OnFailure is null) throw failure; // -> actor supervision

            Self.Tell(effect.OnFailure(failure), sender);
            return;
        }
        finally
        {
            // A timed-out task may still be using scoped services; never dispose
            // its scope underneath it. Release once it actually finishes.
            if (task.IsCompleted)
            {
                cts.Dispose();
                await scope.DisposeAsync();
            }
            else
            {
                ReleaseWhenComplete(task, scope, cts, _log);
            }
        }

        // Completion messages re-enter through the mailbox and the normal Decide path.
        if (completion is not null) Self.Tell(completion, sender);
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
                await scope.DisposeAsync();
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
    internal sealed class StopEntity
    {
        public static readonly StopEntity Instance = new();
        private StopEntity() { }
    }
}
