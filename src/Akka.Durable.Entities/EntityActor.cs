using System.Collections.Immutable;
using Akka.Actor;
using Akka.DependencyInjection;
using Akka.Durable.Abstractions.Entities;
using Akka.Event;
using Akka.Persistence;
using Akka.Reminders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Akka.Durable.Entities;

public abstract class EntityActor<TState, TCommand, TEvent> : ReceivePersistentActor
{
    protected EntityId EntityId { get; }
    protected EntityTypeName EntityType => _entityDefinition.TypeName;

    protected readonly EffectsCancellation

    private readonly EntityDefinition<TState, TCommand, TEvent> _entityDefinition;
    private readonly EntityOptions _entityOptions;
    protected readonly IServiceProvider DependencyResolver;
    protected readonly ITimeProvider TimeProvider;
    protected readonly IReminderClient ReminderClient;
    protected new readonly ILoggingAdapter Log;

    protected EntityActor(EntityId id, EntityDefinition<TState, TCommand, TEvent> entityDefinition,
        IServiceProvider dependencyResolver, ITimeProvider timeProvider, EntityId entityId)
    {
        _entityDefinition = entityDefinition;
        DependencyResolver = dependencyResolver;
        TimeProvider = timeProvider;
        EntityId = entityId;
        _entityOptions = entityDefinition.Options ?? EntityOptions.Default;
        ReminderClient = Context.ReminderClient()
            .CreateClient(_entityDefinition.TypeName.Value, EntityId.Value);

        Log = Context.GetLogger().WithContext("EntityType", _entityDefinition.TypeName)
            .WithContext("EntityId", EntityId);

        PersistenceId = _entityOptions.PersistenceIdFactory(entityId, _entityDefinition.TypeName);

        // populate the initial state
        State = _entityDefinition.InitialState(EntityId);

        Recovers();
        DefaultHandlers();
    }

    public sealed override string PersistenceId { get; }

    public TState State { get; set; }

    private void Recovers()
    {
        Recover<SnapshotOffer>(offer =>
        {
            if (offer.Snapshot is TState state)
            {
                Log.Debug("Recovered snapshot at SeqNr {SeqNr}", offer.Metadata);
                State = state;
            }
        });

        Recover<TEvent>(e => { State = _entityDefinition.Evolve(State, e); });
    }

    private void DefaultHandlers()
    {
        CommandAsync<TCommand>(async command =>
        {
            var sender = Sender;

            var ctx = new DecisionContext(
                EntityId,
                EntityType,
                TimeProvider.Now);

            var decision = _entityDefinition.Decide(
                State, command, ctx);

            switch (decision)
            {
                case Handled<TEvent> handled:
                    await HandleDecisionAsync(handled, sender);
                    break;

                case Ignored:
                    break;

                case Unhandled:
                    Unhandled(command!);
                    break;

                case Stashed:
                    Stash.Stash();
                    break;
            }
        });

        Command<RecoveryCompleted>(c => { Log.Debug("Recovery completed at SeqNr {SeqNr}", LastSequenceNr); });
    }

    private async Task HandleDecisionAsync(Handled<TEvent> handled, IActorRef sender)
    {
        var allEvents = handled.Events;
        PersistAll(allEvents, e => { State = _entityDefinition.Evolve(State, e); }, 
            onCompleteAsync: () => ExecuteEffectsAsync(handled.Effects, sender));
    }

    private async Task ExecuteEffectsAsync(
        ImmutableArray<EntityEffect> effects,
        IActorRef sender)
    {
        foreach (var effect in effects)
        {
            switch (effect)
            {
                case ReplyEffect reply:
                    sender.Tell(reply.Response, Self);
                    break;

                case TellEffect tell:
                    tell.Recipient.Tell(tell.Message);
                    break;

                case RunEffect run:
                    await ExecuteRunAsync(run);
                    break;

                case PipeToEffect pipe:
                    StartPipeTo(pipe);
                    break;

                case StopEffect:
                    Context.Stop(Self);
                    return;
            }
        }
    }

    private async Task ExecuteRunAsync(RunEffect effect)
    {
        await using var scope = DependencyResolver.CreateAsyncScope();

        await effect.ExecuteAsync(
            scope.ServiceProvider,
            _effectCancellation.Token);
    }
}