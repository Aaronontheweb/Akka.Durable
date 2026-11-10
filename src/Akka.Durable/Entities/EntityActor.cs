using System.Collections.Immutable;
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

    // Cancelled when this incarnation stops. In-flight Run and Execute work is asked to stop, and an
    // outcome that arrives afterwards is dropped: the next incarnation re-derives pending effects in Resume.
    private readonly CancellationTokenSource _lifetime = new();

    // Created lazily so entities that never touch reminders don't require them to be configured.
    private IReminderClient? _reminderClient;

    private long _lastSnapshotSequenceNr;

    // Execute handlers this incarnation started whose outcome it has not received yet. Send is not counted:
    // a send has no time limit. Run is awaited in place, so a stop waits for it anyway.
    private int _executesInFlight;

    // The host asked this entity to stop while an Execute handler was in flight. It stops at the end of the
    // first decision that leaves nothing in flight.
    private bool _stopRequested;

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

    private DecisionContext<TState, TCommand, TEvent> NewContext() =>
        new(_entityId, EntityType, _timeProvider.Now);

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
        {
            _log.Debug("Recovery completed at SeqNr {0}", LastSequenceNr);
            ResumePendingEffects();
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

        Command<TCommand>(command => HandleCommand(command, Sender, reminder: null));
        Command<EffectCompleted>(HandleEffectCompleted);
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
    private void HandleCommand(TCommand command, IActorRef origin, ReminderEnvelope? reminder)
    {
        switch (_definition.Decide(State, command, NewContext()))
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

        // The sender gets no reply, and Akka.NET logs an unhandled message only at debug level when asked to.
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

        var outcome = completed.Outcome;
        if (completed.Failure is { } failure)
        {
            if (completed.Failed is null)
            {
                // The state already records that the effect was issued. Resume re-derives it on the next
                // incarnation and a retry arm in Decide can re-issue it sooner. A handler that has to report a
                // failure to the entity returns it as an outcome instead of throwing.
                _log.Warning(failure, "Effect {0} failed for entity [{1}/{2}]. State is unchanged.",
                    completed.Effect, EntityType.Value, _entityId.Value);
                StopIfRequested();
                return;
            }

            // The effect says what a failure means to this entity: it is an outcome like any other.
            _log.Warning(failure, "Effect {0} failed for entity [{1}/{2}]. The entity decides on the failure.",
                completed.Effect, EntityType.Value, _entityId.Value);
            outcome = completed.Failed(failure);
        }

        switch (outcome)
        {
            case TCommand command:
                HandleCommand(command, completed.Origin, reminder: null);
                break;

            case TEvent @event:
                // A fact from the outside world: journaled as it is, no decision involved.
                Commit(Decisions.Persist<TCommand, TEvent>(@event), completed.Origin, reminder: null);
                break;

            case null:
                _log.Debug("Effect {0} completed with no outcome", completed.Effect);
                StopIfRequested();
                break;

            default:
                throw new InvalidOperationException(
                    $"The handler for effect {completed.Effect} returned a {outcome.GetType()}, which is " +
                    $"neither a {typeof(TCommand)} nor a {typeof(TEvent)}.");
        }
    }

    private void Commit(Handled<TCommand, TEvent> handled, IActorRef origin, ReminderEnvelope? reminder)
    {
        ImmutableArray<TEvent> events = handled.Events.IsDefault ? [] : handled.Events;
        ImmutableArray<EntityEffect> effects = handled.Effects.IsDefault ? [] : handled.Effects;

        // Zero events is fine here: PersistAll routes onCompleteAsync through the internal
        // *stashing* Defer, so no further commands are processed until the effects are dispatched.
        // (Public DeferAsync does NOT stash - don't swap it in.)
        PersistAll(events, Apply, onCompleteAsync: async () =>
        {
            if (!events.IsEmpty) MaybeSnapshot();
            await ExecuteEffectsAsync(effects, origin);
            if (reminder is not null) AckReminder(reminder);

            // The decision is complete. It may have started new Execute effects; then the entity keeps running.
            StopIfRequested();
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

    private async Task ExecuteEffectsAsync(ImmutableArray<EntityEffect> effects, IActorRef origin)
    {
        foreach (var effect in effects)
        {
            switch (effect)
            {
                case ReplyEffect reply:
                    origin.Tell(reply.Response, Self);
                    break;

                case TellEffect tell:
                    tell.Recipient.Tell(tell.Message, Self);
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
        catch
        {
            await scope.DisposeAsync();
            throw;
        }

        var self = Self;
        var lifetime = _lifetime.Token;
        var log = _log;
        var effect = execute.Effect;
        var failed = execute.Failed;
        _executesInFlight++;
        _ = InvokeAsync(scope, (_, ct) => invoke(ct), timeout: null).ContinueWith(t =>
        {
            // This incarnation is gone; its successor re-derives pending effects in Resume.
            if (lifetime.IsCancellationRequested) return;

            if (t.IsCompletedSuccessfully)
            {
                self.Tell(new EffectCompleted(effect, t.Result.Result, t.Result.Failure, origin, failed), self);
            }
            else
            {
                log.Error(t.Exception, "The outcome of effect {0} could not be observed", effect);
                // Still report back, so that the entity no longer counts the effect as in flight.
                self.Tell(new EffectCompleted(effect, null, null, origin, null), self);
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
    private sealed record EffectCompleted(
        object Effect, object? Outcome, Exception? Failure, IActorRef Origin, Func<Exception, object?>? Failed)
        : INoSerializationVerificationNeeded;

    /// <summary>Another entity's reply to a <c>Send</c>, with the origin of the command that sent it.</summary>
    private sealed record ForeignReply(object Message, IActorRef Origin) : INoSerializationVerificationNeeded;
}
