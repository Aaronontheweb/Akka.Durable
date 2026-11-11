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
internal sealed class EntityActor<TState, TCommand, TEvent> : ReceivePersistentActor, IWithTimers
{
    /// <summary>
    /// How long after its time limit an attempt without a policy counts as interrupted, and the least time before the
    /// wake when it is stored again. A live attempt has its answer or its give-up by then.
    /// </summary>
    internal static readonly TimeSpan WakeMargin = TimeSpan.FromSeconds(5);

    private readonly EntityId _entityId;
    private readonly EntityDefinition<TState, TCommand, TEvent> _definition;
    private readonly EntityOptions _options;
    private readonly IServiceProvider _services;
    private readonly ITimeProvider _timeProvider;
    private readonly ILoggingAdapter _log;

    // Cancelled when this incarnation stops. In-flight Run and Execute work is asked to stop, and an outcome that arrives
    // afterwards is dropped: the next incarnation attempts a recorded effect again or gives it up.
    private readonly CancellationTokenSource _lifetime = new();

    // Where each effect type is handled. Registered with AddEffectHandler.
    private readonly EffectRoutes _routes;

    // Recorded effects that were issued and have no acknowledgement or give-up yet, by id. Kept by the persist handlers
    // and rebuilt in recovery from the journal records and the snapshot.
    private readonly SortedDictionary<long, PendingEffect> _pending = [];

    // Who the answer of a pending effect replies to: the sender of the command that issued it. Dead letters after a
    // restart. Kept in memory only.
    private readonly Dictionary<long, IActorRef> _origins = [];

    // When the next attempt of a pending effect that waits between attempts is due, in this incarnation.
    private readonly Dictionary<long, DateTimeOffset> _retryAt = [];

    // True while this incarnation knows the wake reminder is stored. It is stored before the first write that leaves an
    // effect pending, and removed once nothing is pending.
    private bool _wakeStored;

    // When the stored wake is due. A write that leaves an effect pending with an earlier next attempt stores it again.
    private DateTimeOffset _wakeDue = DateTimeOffset.MaxValue;

    // Created lazily so entities that never touch reminders don't require them to be configured.
    private IReminderClient? _reminderClient;

    private long _lastSnapshotSequenceNr;

    // Execute handlers this incarnation started whose outcome it has not received yet. Send is not counted:
    // a send has no time limit. Run is awaited in place, so a stop waits for it anyway. Waiting between two attempts
    // does not count.
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

    public ITimerScheduler Timers { get; set; } = null!;

    private EntityTypeName EntityType => _definition.TypeName;

    private IReminderClient Reminders =>
        _reminderClient ??= Context.ReminderClient().CreateClient(EntityType.Value, _entityId.Value);

    private DecisionContext<TState, TCommand, TEvent> NewContext() =>
        new(_entityId, EntityType, _timeProvider.Now) { Routes = _routes };

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
        Recover<EffectIssued>(issued => Track(issued, LastSequenceNr));
        Recover<EffectRetried>(Track);
        Recover<EffectAcknowledged>(acknowledged => _pending.Remove(acknowledged.Id));
        Recover<EffectGaveUp>(gaveUp => _pending.Remove(gaveUp.Id));

        // Replay only domain events through Evolve. Decide and effects never run during recovery.
        // A C# 15 union TEvent comes back from the journal as the union, because WithDurableEntity requires
        // it to have its own serializer (a union marked [AkkaSerializable(Manifest = ...)]).
        Recover<TEvent>(Apply);

        Recover<RecoveryCompleted>(_ =>
        {
            _log.Debug("Recovery completed at SeqNr {0}", LastSequenceNr);
            ContinuePendingEffects();
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

    private void Track(EffectIssued issued, long sequence) =>
        _pending[sequence] = new PendingEffect(sequence, issued.Effect, issued.IssuedAt, issued.Policy, 1, issued.NextAt);

    private void Track(EffectRetried retried)
    {
        if (_pending.TryGetValue(retried.Id, out var pending))
            _pending[retried.Id] = pending with { Attempt = retried.Attempt, NextAt = retried.NextAt };
    }

    /// <summary>
    /// After recovery, every recorded effect that is still pending had its last attempt cut off: the previous
    /// incarnation stopped before the attempt's answer or give-up was journaled. An effect without a retry policy is
    /// not run again: its give-up with <see cref="GaveUpReason.Interrupted"/> is decided. An effect whose policy has run
    /// out gives up with <see cref="GaveUpReason.RetriesExhausted"/>. These are decided one after the other, in the
    /// order the effects were issued, each with its record in the decision's write. Every other effect gets its next
    /// attempt when it is due. Then the wake is stored again, and the library's own work after recovery runs.
    /// </summary>
    /// <remarks>
    /// This all happens before the internal stash is released, so before any command that arrived during recovery.
    /// Each decision is persisted inside the completion of the one before, so each sees the state the one before left.
    /// </remarks>
    private void ContinuePendingEffects()
    {
        // The previous incarnation stored a wake when it left these effects pending. It is removed once nothing is
        // pending, and stored again after recovery while something still is.
        if (_pending.Count > 0)
        {
            _wakeStored = true;
            _wakeDue = DateTimeOffset.MaxValue;
        }

        var giveUps = new List<(PendingEffect Pending, GaveUpReason Reason)>();
        foreach (var pending in _pending.Values)
        {
            var policy = pending.Policy?.ToPolicy();
            if (policy is null)
                giveUps.Add((pending, GaveUpReason.Interrupted));
            else if (!policy.Allows(pending.Attempt + 1))
                giveUps.Add((pending, GaveUpReason.RetriesExhausted));
            else
                ScheduleRetry(pending.Sequence, pending.Attempt + 1, pending.NextAt);
        }

        if (giveUps.Count > 0)
            _log.Info("{0} effects of entity [{1}/{2}] were cut off by a restart and give up; {3} are attempted again.",
                giveUps.Count, EntityType.Value, _entityId.Value, _pending.Count - giveUps.Count);

        GiveUpAfterRecovery(giveUps, 0);
    }

    private void GiveUpAfterRecovery(List<(PendingEffect Pending, GaveUpReason Reason)> giveUps, int index)
    {
        if (index == giveUps.Count)
        {
            AfterRecovery();
            return;
        }

        var (pending, reason) = giveUps[index];
        GiveUp(pending, reason, error: null, Context.System.DeadLetters, then: () => GiveUpAfterRecovery(giveUps, index + 1));
    }

    /// <summary>
    /// The end of recovery: the wake is stored again while effects are pending, so a crash in this incarnation is
    /// covered too. Then the library's own work for the recovered state runs, for a workflow host.
    /// </summary>
    private void AfterRecovery()
    {
        if (_pending.Count > 0)
            DeferStashing(async () =>
            {
                await TryStoreWakeAsync(TimeSpan.Zero);
                RunAfterRecovery();
            });
        else
            RunAfterRecovery();
    }

    private void RunAfterRecovery()
    {
        if (_definition.AfterRecovery is null) return;

        var pending = _definition.AfterRecovery(State, NewContext());
        if (pending.Effects.IsDefaultOrEmpty && pending.Events.IsDefaultOrEmpty) return;

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
            Sender.Tell(new PendingEffectsReply([.. _pending.Values.Select(pending => PublicForm(pending.Effect))])));

        Command<TCommand>(command => HandleCommand(command, Sender, reminder: null));
        Command<EffectCompleted>(HandleEffectCompleted);
        Command<RetryDue>(HandleRetryDue);
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
    /// <param name="settlement">
    /// For the answer or the give-up of a recorded effect: the record that settles it, written in the same write as the
    /// decision's events, whatever the decision is.
    /// </param>
    private void HandleCommand(
        TCommand command, IActorRef origin, ReminderEnvelope? reminder, Settlement? settlement = null)
    {
        var decision = _definition.Decide(State, command, NewContext());
        if (settlement is not null)
        {
            ImmutableArray<object> records = [settlement.Record];
            if (decision is Handled<TCommand, TEvent> handled)
            {
                Commit(handled, origin, reminder, records, settlement.Then);
                return;
            }

            // The decision runs nothing, but the settlement must still be journaled.
            if (decision is Stashed)
            {
                // The current message may not be this command (after recovery it is not). The command is decided
                // again when the stash is released, and the settlement is journaled then.
                Stash.Prepend([new Envelope(new DecideAgain(settlement.Sequence, command!, settlement.Record, origin), Self)]);
                records = [];
            }
            else if (decision is NotHandled) Unhandled(command);
            else if (decision is not Ignored)
                throw new InvalidOperationException(
                    $"{EntityType.Value}.Decide returned an uninitialized Decision for command {command}.");

            Commit(Decisions.None<TCommand, TEvent>(), origin, reminder, records, settlement.Then);
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
        if (envelope.Message is EffectsWake)
        {
            HandleWake(envelope);
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

    /// <summary>The outcome of an effect that is not recorded: a workflow's own work.</summary>
    private void HandleEffectCompleted(EffectCompleted completed)
    {
        // An outcome sent just before a restart can reach the next incarnation, which never counted it.
        _executesInFlight = Math.Max(0, _executesInFlight - 1);

        if (completed.Sequence > 0)
        {
            SettleAttempt(completed);
            return;
        }

        var outcome = completed.Outcome;
        if (completed.Failure is { } failure)
        {
            if (completed.Failed is null)
            {
                // Nothing tells the entity about the failure: it is logged and dropped.
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
                HandleCommand(command, completed.Origin, reminder: null);
                break;

            case null:
                _log.Debug("Effect {0} completed with no outcome", completed.Effect.GetType().Name);
                StopIfRequested();
                break;

            default:
                throw new InvalidOperationException(
                    $"The handler for effect {completed.Effect.GetType().Name} returned a {outcome.GetType()}, which is " +
                    $"not a {typeof(TCommand)}. A handler returns one of the entity's commands.");
        }
    }

    /// <summary>
    /// An attempt of a recorded effect finished. Its answer is decided with an <see cref="EffectAcknowledged"/> in the
    /// same write; a handler with no answer is acknowledged in a write of its own. A failure, a time-out or an empty
    /// answer is a failed attempt: the effect is attempted again if its policy allows, otherwise it gives up and its
    /// give-up command is decided with an <see cref="EffectGaveUp"/>.
    /// </summary>
    private void SettleAttempt(EffectCompleted completed)
    {
        var sequence = completed.Sequence;
        var effect = completed.Effect;
        if (!_pending.TryGetValue(sequence, out var pending) || pending.Attempt != completed.Attempt)
        {
            // Settled already, or an answer of an earlier attempt: one answer per effect.
            _log.Debug("Dropping the late answer of attempt {0} of effect {1} (id {2}) of entity [{3}/{4}].",
                completed.Attempt, effect.GetType().Name, sequence, EntityType.Value, _entityId.Value);
            StopIfRequested();
            return;
        }

        var delivery = DeliveryOf(pending.Effect);
        var failure = completed.Failure;
        TCommand answer = default!;
        if (failure is null && delivery is { Answers: true })
        {
            if (completed.Outcome is TCommand command && command is not IUnion { Value: null })
                answer = command;
            else
                failure = new InvalidOperationException(completed.Outcome is null or IUnion { Value: null }
                    ? $"The handler of {delivery.Name} answered with nothing."
                    : $"The handler of {delivery.Name} answered with a {completed.Outcome.GetType().Name}, " +
                      $"which is not a {typeof(TCommand).Name}.");
        }

        var origin = OriginOf(sequence);
        if (failure is null)
        {
            Forget(sequence);
            var acknowledged = new EffectAcknowledged(sequence);
            if (delivery is { Answers: true })
                HandleCommand(answer, origin, reminder: null, new Settlement(sequence, acknowledged, Then: null));
            else
                Commit(Decisions.None<TCommand, TEvent>(), origin, reminder: null, [acknowledged]);
            return;
        }

        var policy = pending.Policy?.ToPolicy();
        if (policy is not null && policy.Allows(pending.Attempt + 1))
        {
            var wait = policy.WaitAfter(pending.Attempt);
            _log.Warning(failure, "Attempt {0} of effect {1} (id {2}) of entity [{3}] failed. Attempt {4} runs in {5}.",
                pending.Attempt, PublicName(pending.Effect), sequence, $"{EntityType.Value}/{_entityId.Value}",
                pending.Attempt + 1, wait);
            ScheduleRetry(sequence, pending.Attempt + 1, _timeProvider.Now + wait);
            StopIfRequested();
            return;
        }

        var reason = policy is not null
            ? GaveUpReason.RetriesExhausted
            : failure is TimeoutException ? GaveUpReason.TimedOut : GaveUpReason.Failed;
        _log.Warning(failure, "Effect {0} (id {1}) of entity [{2}/{3}] gave up as {4} after {5} attempts. Decide gets its give-up command.",
            PublicName(pending.Effect), sequence, EntityType.Value, _entityId.Value, reason, pending.Attempt);
        GiveUp(pending, reason, failure.Message, origin, then: null);
    }

    /// <summary>
    /// Decides the give-up of <paramref name="pending"/>: the command its effect maps it to, with an
    /// <see cref="EffectGaveUp"/> in the same write. A library start has no command; its record is journaled alone.
    /// </summary>
    private void GiveUp(PendingEffect pending, GaveUpReason reason, string? error, IActorRef origin, Action? then)
    {
        var sequence = pending.Sequence;
        Forget(sequence);
        var record = new EffectGaveUp(sequence, pending.Attempt, reason);
        var gaveUp = new WorkGaveUp(PublicForm(pending.Effect), reason, error, pending.Attempt);

        object? command;
        if (pending.Effect is IEffect<TCommand> effect)
            command = effect.GaveUp(gaveUp);
        else
            command = DeliveryOf(pending.Effect)?.GaveUp?.Invoke(gaveUp);

        if (command is TCommand decided)
        {
            HandleCommand(decided, origin, reminder: null, new Settlement(sequence, record, then));
            return;
        }

        // The effect's type changed since it was journaled, or the call has no command for a give-up.
        _log.Error("Effect {0} (id {1}) of entity [{2}/{3}] gave up as {4}, and Decide has no command for it. It is " +
                   "closed.", PublicName(pending.Effect), sequence, EntityType.Value, _entityId.Value, reason);
        Commit(Decisions.None<TCommand, TEvent>(), origin, reminder: null, [record], then);
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
            new Settlement(again.Sequence, again.Record, Then: null));
    }

    private void LogFailedEffect(object effect, Exception failure, string consequence)
    {
        if (failure is MissingEffectHandlerException missing)
            LogMissingHandler(missing);
        else
            _log.Warning(failure, "Effect {0} failed for entity [{1}/{2}]. {3}",
                effect.GetType().Name, EntityType.Value, _entityId.Value, consequence);
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
    /// <remarks>
    /// When the write leaves an effect pending and the wake is not stored yet, the wake is stored first. If that fails,
    /// nothing is written, the command fails, and the decision's other effects do not run.
    /// </remarks>
    /// <param name="handled">The decision.</param>
    /// <param name="origin">Who <c>Reply</c> answers.</param>
    /// <param name="reminder">The reminder delivery that carried the command, acknowledged once it is decided.</param>
    /// <param name="records">Library records that settle effects or start an attempt.</param>
    /// <param name="then">Runs last, after the effects. Used to decide the next give-up after recovery.</param>
    private void Commit(
        Handled<TCommand, TEvent> handled, IActorRef origin, ReminderEnvelope? reminder,
        ImmutableArray<object> records = default, Action? then = null)
    {
        ImmutableArray<TEvent> events = handled.Events.IsDefault ? [] : handled.Events;
        ImmutableArray<EntityEffect> effects = handled.Effects.IsDefault ? [] : handled.Effects;

        var now = _timeProvider.Now;
        var issued = new List<IssuedEffect>();
        foreach (var effect in effects)
            if (effect is ExecuteEffect { Recorded: true } execute)
                issued.Add(Issue(execute, now));

        var earliestIssued = issued.Where(effect => effect.Record is not null).Select(effect => effect.Record!.NextAt)
            .DefaultIfEmpty(DateTimeOffset.MaxValue).Min();
        if (earliestIssued != DateTimeOffset.MaxValue && (!_wakeStored || earliestIssued < _wakeDue))
        {
            // The wake is stored before the write, so no crash can leave an effect with nothing to wake the entity.
            // Inside a stashing defer, so no other command is decided in between.
            var others = NextWake(TimeSpan.Zero);
            var due = earliestIssued < others ? earliestIssued : others;
            DeferStashing(async () =>
            {
                try
                {
                    await StoreWakeAsync(due);
                }
                catch (Exception error)
                {
                    _log.Error(error, "Entity [{0}/{1}] could not store the reminder that wakes it after a crash, so " +
                                      "the decision that issues an effect is not journaled and the command fails.",
                        EntityType.Value, _entityId.Value);
                    if (!origin.IsNobody() && !origin.Equals(Context.System.DeadLetters))
                        origin.Tell(new Status.Failure(error), Self);
                    if (reminder is not null) NackReminder(reminder, "The wake reminder could not be stored");
                    StopIfRequested();
                    then?.Invoke();
                    return;
                }

                Write(events, effects, issued, records, origin, reminder, then);
            });
            return;
        }

        Write(events, effects, issued, records, origin, reminder, then);
    }

    private void Write(
        ImmutableArray<TEvent> events, ImmutableArray<EntityEffect> effects, List<IssuedEffect> issued,
        ImmutableArray<object> records, IActorRef origin, ReminderEnvelope? reminder, Action? then)
    {
        var journaled = issued.Where(effect => effect.Record is not null).ToList();
        var batch = ImmutableArray.CreateBuilder<object>(events.Length + (records.IsDefault ? 0 : records.Length) + journaled.Count);
        foreach (var @event in events) batch.Add(@event!);
        if (!records.IsDefault) batch.AddRange(records);
        foreach (var effect in journaled) batch.Add(effect.Record!);

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
                    journaled[next++].Sequence = sequence;
                    Track(record, sequence);
                    _origins[sequence] = origin;
                    break;
                case EffectRetried retried:
                    Track(retried);
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
            if (batch.Count > 0) MaybeSnapshot();
            await ExecuteEffectsAsync(effects, origin, issued);
            if (reminder is not null) AckReminder(reminder);

            // Nothing is pending any more: the wake is not needed. Removed best effort, before the next command, so
            // a new effect stores it again.
            if (_pending.Count == 0 && _wakeStored) await RemoveWakeAsync();

            // The decision is complete. It may have started new Execute effects; then the entity keeps running.
            StopIfRequested();
            then?.Invoke();
        });
    }

    /// <summary>
    /// Prepares the record of a recorded <c>Execute</c>. A call to a workflow the definition does not declare, or a
    /// notify or cancel the definition cannot map, is not journaled: it is logged and dropped. An effect type with no
    /// handler is refused before the write: a configuration error, so it goes to supervision.
    /// </summary>
    private IssuedEffect Issue(ExecuteEffect execute, DateTimeOffset now)
    {
        object stored;
        RetryPolicy? policy;
        if (execute.Effect is IWorkflowCall call)
        {
            if (!_definition.Used.TryGetValue(call.Workflow, out var use) || !use.Matches(call))
            {
                // The call's contents may hold personal data, so only the workflow type and id are named.
                _log.Error("Entity [{0}/{1}] called workflow [{2}/{3}], but its definition does not name workflow type " +
                           "[{2}] with Uses. The call was not sent. Add .Uses(...) with that workflow's definition to the " +
                           "entity definition.",
                    EntityType.Value, _entityId.Value, call.Workflow.Value, call.Id.Value);
                return new IssuedEffect(execute, Record: null);
            }

            if (call is not StartWorkflow && !use.Answers)
            {
                _log.Error("Entity [{0}/{1}] notified or cancelled workflow [{2}/{3}], but its definition uses workflow type " +
                           "[{2}] without told and gaveUp. The call was not sent. Declare it with .Uses(definition, told: ..., " +
                           "gaveUp: ...), so the answer and the give-up reach Decide, also after a restart.",
                    EntityType.Value, _entityId.Value, call.Workflow.Value, call.Id.Value);
                return new IssuedEffect(execute, Record: null);
            }

            stored = call.Recorded;
            policy = call is StartWorkflow ? WorkflowUse.StartPolicy : null;
        }
        else
        {
            if (!_routes.TryGet(execute.Effect.GetType(), out var route))
            {
                var missing = new MissingEffectRouteException(execute.Effect.GetType(), typeof(TCommand));
                _log.Error("Entity [{0}/{1}] issued the effect {2}, and no handler is registered for it. Register it at " +
                           "startup with services.{3}. The effect was not journaled and not run.",
                    EntityType.Value, _entityId.Value, missing.EffectType, missing.Registration);
                throw missing;
            }

            stored = execute.Effect;
            policy = route.Policy;
        }

        return new IssuedEffect(execute,
            new EffectIssued(stored, now, StoredPolicy.Of(policy), NextAttemptAt(now, policy, attempt: 1)));
    }

    /// <summary>
    /// When the attempt after <paramref name="attempt"/> is due if <paramref name="attempt"/>, started at
    /// <paramref name="start"/>, gets no answer. Without a policy: when the attempt counts as interrupted.
    /// </summary>
    private DateTimeOffset NextAttemptAt(DateTimeOffset start, RetryPolicy? policy, int attempt) =>
        start + _options.EffectTimeout + (policy?.WaitAfter(attempt) ?? WakeMargin);

    /// <summary>
    /// The host asks the entity to stop. With an <c>Execute</c> handler in flight the entity stops later, once
    /// it has decided on every outcome. The host buffers the entity's messages until then.
    /// </summary>
    /// <remarks>
    /// <para>While effects are pending, the wake is stored again first, due when the next attempt is, so the entity is
    /// woken to make it.</para>
    /// <para>The wait is bounded during a rebalance or a node shutdown: the shard stops the entity itself once its
    /// hand-off timeout has passed.</para>
    /// </remarks>
    private void RequestStop()
    {
        _stopRequested = true;
        if (_executesInFlight > 0)
            _log.Debug("Stop deferred until {0} Execute effects in flight have completed", _executesInFlight);

        if (_pending.Count == 0)
        {
            StopIfRequested();
            return;
        }

        DeferStashing(async () =>
        {
            await TryStoreWakeAsync(TimeSpan.Zero);
            StopIfRequested();
        });
    }

    /// <summary>Stops the entity if its host asked it to and no <c>Execute</c> handler is in flight.</summary>
    private void StopIfRequested()
    {
        if (_stopRequested && _executesInFlight == 0) Context.Stop(Self);
    }

    /// <summary>
    /// Runs <paramref name="action"/> after everything persisted so far, with commands stashed until it has finished.
    /// It may persist again.
    /// </summary>
    private void DeferStashing(Func<Task> action) =>
        PersistAll(Array.Empty<object>(), _ => { }, onCompleteAsync: action);

    #endregion

    #region Delivery of recorded effects

    /// <summary>How to deliver a recorded effect, found from the effect itself, so also after a restart.</summary>
    /// <param name="Name">The effect's type name, for logs.</param>
    /// <param name="Invoke">One attempt. Returns the entity's command, or null when the delivery has no answer.</param>
    /// <param name="Answers">True when an attempt answers with a command for <c>Decide</c>.</param>
    /// <param name="GaveUp">The command a give-up becomes, for a library call. Null for the author's effects.</param>
    private sealed record Delivery(
        string Name,
        Func<IServiceProvider, EffectDelivery, CancellationToken, Task<object?>> Invoke,
        bool Answers,
        Func<WorkGaveUp, object?>? GaveUp);

    /// <summary>
    /// The delivery of a stored effect. Null when it cannot be delivered any more: its handler is no longer registered,
    /// or the definition no longer uses the workflow.
    /// </summary>
    private Delivery? DeliveryOf(object stored)
    {
        switch (stored)
        {
            case RecordedStart start when UseOf(start.Workflow) is { } use:
                return new Delivery(nameof(StartWorkflow), async (services, _, ct) =>
                {
                    await use.Start(services, new WorkflowId(start.Id), start.Input, ct);
                    return null;
                }, Answers: false, GaveUp: null);

            case RecordedNotify notify when UseOf(notify.Workflow) is { Notify: { } send } use:
                return new Delivery(nameof(NotifyWorkflow),
                    (services, _, ct) => send(services, new WorkflowId(notify.Id), notify.Fact, ct),
                    Answers: true, use.GaveUp);

            case RecordedCancel cancel when UseOf(cancel.Workflow) is { Cancel: { } send } use:
                return new Delivery(nameof(CancelWorkflow),
                    (services, _, ct) => send(services, new WorkflowId(cancel.Id), cancel.Reason, cancel.By, ct),
                    Answers: true, use.GaveUp);

            case RecordedStart or RecordedNotify or RecordedCancel:
                return null;

            default:
                return _routes.TryGet(stored.GetType(), out var route)
                    ? new Delivery(stored.GetType().Name,
                        (services, delivery, ct) => route.Invoke(services, stored, delivery, ct),
                        Answers: route.OutcomeType is not null, GaveUp: null)
                    : null;
        }
    }

    private WorkflowUse? UseOf(string workflow) =>
        _definition.Used.TryGetValue(new WorkflowTypeName(workflow), out var use) ? use : null;

    /// <summary>The effect as a decision issued it: a call to a workflow is shown as its public record.</summary>
    private object PublicForm(object stored) =>
        stored switch
        {
            RecordedStart start => new StartWorkflow(new WorkflowTypeName(start.Workflow), new WorkflowId(start.Id), start.Input)
            {
                RegionKey = UseOf(start.Workflow)?.RegionKey ?? typeof(object),
            },
            RecordedNotify notify => new NotifyWorkflow(new WorkflowTypeName(notify.Workflow), new WorkflowId(notify.Id), notify.Fact)
            {
                RegionKey = UseOf(notify.Workflow)?.RegionKey ?? typeof(object),
            },
            RecordedCancel cancel => new CancelWorkflow(new WorkflowTypeName(cancel.Workflow), new WorkflowId(cancel.Id),
                cancel.Reason, cancel.By)
            {
                RegionKey = UseOf(cancel.Workflow)?.RegionKey ?? typeof(object),
            },
            _ => stored,
        };

    // The effect's contents may hold personal data, so logs name only its type.
    private string PublicName(object stored) => PublicForm(stored).GetType().Name;

    private IActorRef OriginOf(long sequence) =>
        _origins.TryGetValue(sequence, out var origin) ? origin : Context.System.DeadLetters;

    private void Forget(long sequence)
    {
        _origins.Remove(sequence);
        _retryAt.Remove(sequence);
        Timers.Cancel(RetryKey(sequence));
    }

    private static string RetryKey(long sequence) => $"durable-retry-{sequence}";

    /// <summary>Attempt <paramref name="attempt"/> of the effect <paramref name="sequence"/> runs at <paramref name="at"/>.</summary>
    private void ScheduleRetry(long sequence, int attempt, DateTimeOffset at)
    {
        var wait = at - _timeProvider.Now;
        _retryAt[sequence] = at;
        Timers.StartSingleTimer(RetryKey(sequence), new RetryDue(sequence, attempt), wait > TimeSpan.Zero ? wait : TimeSpan.Zero);
    }

    /// <summary>
    /// The next attempt of an effect is due. It is journaled as an <see cref="EffectRetried"/> before it runs, so a
    /// restart does not reset the count. An effect that cannot be delivered any more gives up as failed.
    /// </summary>
    private void HandleRetryDue(RetryDue due)
    {
        if (!_pending.TryGetValue(due.Sequence, out var pending) || pending.Attempt != due.Attempt - 1)
            return;

        _retryAt.Remove(due.Sequence);
        var origin = OriginOf(due.Sequence);
        if (DeliveryOf(pending.Effect) is null)
        {
            _log.Error("Effect {0} (id {1}) of entity [{2}/{3}] has no handler any more, so it cannot be attempted again. " +
                       "It gives up as Failed.", PublicName(pending.Effect), due.Sequence, EntityType.Value, _entityId.Value);
            GiveUp(pending, GaveUpReason.Failed, "No handler is registered for the effect.", origin, then: null);
            return;
        }

        var policy = pending.Policy?.ToPolicy();
        var retried = new EffectRetried(due.Sequence, due.Attempt, NextAttemptAt(_timeProvider.Now, policy, due.Attempt));
        Commit(Decisions.None<TCommand, TEvent>(), origin, reminder: null, [retried],
            then: () => StartAttempt(due.Sequence, origin));
    }

    /// <summary>Runs the current attempt of a pending effect. Its outcome comes back as an <see cref="EffectCompleted"/>.</summary>
    private void StartAttempt(long sequence, IActorRef origin)
    {
        if (!_pending.TryGetValue(sequence, out var pending)) return;

        if (DeliveryOf(pending.Effect) is not { } delivery)
        {
            GiveUp(pending, GaveUpReason.Failed, "No handler is registered for the effect.", origin, then: null);
            return;
        }

        var id = new EffectDelivery(
            new EffectId($"{EntityType.Value}/{_entityId.Value}/{sequence}", sequence), pending.Attempt);
        var scope = _services.CreateAsyncScope();
        var services = scope.ServiceProvider;
        StartHandler(scope, ct => delivery.Invoke(services, id, ct), pending.Effect, origin, failed: null, sequence,
            pending.Attempt);
    }

    #endregion

    #region The wake

    /// <summary>
    /// The wake fired. While effects are pending it is stored again, due when the next of them is, and only then
    /// acknowledged: the acknowledgement of a delivery leaves a newer reminder with the same key alone. The pending
    /// effects themselves were already taken up when the entity recovered, or are on their timers.
    /// </summary>
    private void HandleWake(ReminderEnvelope envelope)
    {
        _wakeStored = false;
        _wakeDue = DateTimeOffset.MaxValue;
        DeferStashing(async () =>
        {
            if (_pending.Count > 0) await TryStoreWakeAsync(WakeMargin);
            AckReminder(envelope);
            StopIfRequested();
        });
    }

    /// <summary>
    /// When the wake must fire: when the earliest pending effect needs its next attempt or its give-up, and not before
    /// <paramref name="floor"/> from now.
    /// </summary>
    /// <param name="floor">
    /// Zero when the wake is stored for a stop or a write. <see cref="WakeMargin"/> when the wake fired on an entity
    /// that is running: its effects are on their timers, so a wake that is already due would only fire again at once.
    /// </param>
    private DateTimeOffset NextWake(TimeSpan floor)
    {
        var earliest = DateTimeOffset.MaxValue;
        foreach (var pending in _pending.Values)
        {
            var due = _retryAt.TryGetValue(pending.Sequence, out var at) ? at : pending.NextAt;
            if (due < earliest) earliest = due;
        }

        var soonest = _timeProvider.Now + floor;
        return earliest > soonest ? earliest : soonest;
    }

    private async Task StoreWakeAsync(DateTimeOffset due)
    {
        EnsureScheduled(await Reminders.ScheduleSingleReminderAsync(
            new ReminderKey(EffectsWake.Key), due, new EffectsWake(due), ct: _lifetime.Token));
        _wakeStored = true;
        _wakeDue = due;
    }

    /// <summary>Stores the wake for the pending effects. A failure is logged: the effects are on their timers anyway.</summary>
    private async Task TryStoreWakeAsync(TimeSpan floor)
    {
        try
        {
            await StoreWakeAsync(NextWake(floor));
        }
        catch (Exception error) when (!_lifetime.IsCancellationRequested)
        {
            _log.Error(error, "Entity [{0}/{1}] could not store the reminder that wakes it for its {2} pending effects. " +
                              "They are attempted while it runs; after a crash they wait for its next message.",
                EntityType.Value, _entityId.Value, _pending.Count);
        }
    }

    private async Task RemoveWakeAsync()
    {
        _wakeStored = false;
        _wakeDue = DateTimeOffset.MaxValue;
        try
        {
            var cancelled = await Reminders.CancelReminderAsync(new ReminderKey(EffectsWake.Key), _lifetime.Token);
            if (cancelled.ResponseCode is not (ReminderCancelResponseCode.Success or ReminderCancelResponseCode.NotFound))
                _log.Warning("Failed to remove the wake of entity [{0}/{1}]: {2} {3}. A wake that fires with nothing " +
                             "pending does nothing.", EntityType.Value, _entityId.Value, cancelled.ResponseCode, cancelled.Message);
        }
        catch (Exception error) when (!_lifetime.IsCancellationRequested)
        {
            _log.Warning(error, "Failed to remove the wake of entity [{0}/{1}]. A wake that fires with nothing pending " +
                                "does nothing.", EntityType.Value, _entityId.Value);
        }
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
                    var current = issued[recorded++];
                    if (current.Record is null) break; // refused before the write, and logged
                    if (current.Sequence == 0)
                    {
                        _log.Error("The effect {0} of entity [{1}/{2}] was not journaled, so it is not run.",
                            current.Execute.Effect.GetType().Name, EntityType.Value, _entityId.Value);
                        break;
                    }

                    StartAttempt(current.Sequence, origin);
                    break;

                case ReplyEffect reply:
                    origin.Tell(reply.Response, Self);
                    break;

                case TellEffect tell:
                    tell.Recipient.Tell(tell.Message, Self);
                    break;

                case ExecuteEffect execute:
                    // A workflow's own work. Started, not awaited: the outcome returns as an EffectCompleted message.
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

        StartHandler(scope, invoke, execute.Effect, origin, execute.Failed, sequence: 0, attempt: 0);
    }

    /// <summary>
    /// Starts a handler and does not wait for it: its outcome comes back as an <see cref="EffectCompleted"/>.
    /// </summary>
    /// <param name="sequence">The id of a recorded effect, or 0.</param>
    /// <param name="attempt">The attempt of a recorded effect, or 0.</param>
    private void StartHandler(
        AsyncServiceScope scope, Func<CancellationToken, Task<object?>> invoke, object effect, IActorRef origin,
        Func<Exception, object?>? failed, long sequence, int attempt)
    {
        var self = Self;
        var lifetime = _lifetime.Token;
        var log = _log;
        _executesInFlight++;
        _ = InvokeAsync(scope, (_, ct) => invoke(ct), timeout: null).ContinueWith(t =>
        {
            // This incarnation is gone. Its successor attempts a recorded effect again or gives it up.
            if (lifetime.IsCancellationRequested) return;

            if (t.IsCompletedSuccessfully)
            {
                self.Tell(new EffectCompleted(effect, t.Result.Result, t.Result.Failure, origin, failed, sequence, attempt), self);
            }
            else
            {
                log.Error(t.Exception, "The outcome of effect {0} could not be observed", effect.GetType().Name);
                // Still report back, so that the entity no longer counts the effect as in flight. A recorded effect
                // counts it as a failed attempt.
                self.Tell(new EffectCompleted(effect, null, sequence > 0 ? t.Exception : null, origin, null, sequence, attempt), self);
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
                    log.Warning(t.Exception, "Send of {0} to [{1}] got no reply", send.Command.GetType().Name, send.Target.Value);
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
    /// <param name="Attempt">The attempt of a recorded effect, or 0.</param>
    private sealed record EffectCompleted(
        object Effect, object? Outcome, Exception? Failure, IActorRef Origin, Func<Exception, object?>? Failed,
        long Sequence = 0, int Attempt = 0)
        : INoSerializationVerificationNeeded;

    /// <summary>The timer of the next attempt of a recorded effect fired.</summary>
    private sealed record RetryDue(long Sequence, int Attempt) : INoSerializationVerificationNeeded;

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

    /// <summary>
    /// A recorded <c>Execute</c> of a decision, its record, and the id the record got in the write. A null record: the
    /// call was refused before the write.
    /// </summary>
    private sealed record IssuedEffect(ExecuteEffect Execute, EffectIssued? Record)
    {
        /// <summary>The sequence number of its <see cref="EffectIssued"/>. 0 until the write succeeds.</summary>
        public long Sequence { get; set; }
    }

    /// <summary>Another entity's reply to a <c>Send</c>, with the origin of the command that sent it.</summary>
    private sealed record ForeignReply(object Message, IActorRef Origin) : INoSerializationVerificationNeeded;
}
