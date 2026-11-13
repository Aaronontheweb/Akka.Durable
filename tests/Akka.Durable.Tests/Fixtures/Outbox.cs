using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace Akka.Durable.Tests.Fixtures;

using OutboxContext = DecisionContext<OutboxState, OutboxCommand, OutboxEvent>;

// A small entity for specs that need to see what a give-up carries: its reason, its attempts and its error.
// It is used on the entity actor directly, so its types need no serializer.

public sealed record OutboxState(ImmutableArray<string> Log);

public sealed record SendMail(string Name);

/// <summary>The answer of <see cref="Deliver"/>: the attempt that was acknowledged.</summary>
public sealed record MailDelivered(string Name, int Attempt);

public sealed record MailGaveUp(string Name, GaveUpReason Reason, int Attempts, string? Error);

public readonly union OutboxCommand(SendMail, MailDelivered, MailGaveUp);

public sealed record Logged(string Line);

public readonly union OutboxEvent(Logged);

public sealed record Deliver(string Name);

/// <summary>What the mail handler saw, and what it does on each attempt.</summary>
public sealed class OutboxWorld
{
    public ConcurrentQueue<EffectDelivery> Deliveries { get; } = new();

    /// <summary>Runs on each attempt after it was recorded. Throw to fail the attempt. Default: acknowledges it.</summary>
    public Func<EffectDelivery, CancellationToken, Task> OnAttempt { get; set; } = (_, _) => Task.CompletedTask;
}

public sealed class DeliverHandler(OutboxWorld world) : IEffectHandler<Deliver, OutboxCommand>
{
    public async Task<OutboxCommand> HandleAsync(Deliver effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        world.Deliveries.Enqueue(delivery);
        await world.OnAttempt(delivery, cancellationToken);
        return new MailDelivered(effect.Name, delivery.Attempt);
    }
}

public static class Outbox
{
    public static readonly EntityDefinition<OutboxState, OutboxCommand, OutboxEvent> Definition =
        new EntityDefinition<OutboxState, OutboxCommand, OutboxEvent>(
                new EntityTypeName("outbox"), _ => new OutboxState([]), Decide, Evolve)
            .GaveUp(g => new MailGaveUp(((Deliver)g.Work).Name, g.Reason, g.Attempts, g.Error));

    private static Decision<OutboxCommand, OutboxEvent> Decide(OutboxState state, OutboxCommand command, OutboxContext ctx) =>
        command switch
        {
            SendMail c => ctx.None().Execute(new Deliver(c.Name)),
            MailDelivered d => ctx.Persist(new Logged($"delivered:{d.Name}:{d.Attempt}")),
            MailGaveUp g => ctx.Persist(new Logged($"gaveup:{g.Name}:{g.Reason}:{g.Attempts}:{g.Error}")),
        };

    private static OutboxState Evolve(OutboxState state, OutboxEvent @event) =>
        @event switch
        {
            Logged l => new OutboxState(state.Log.Add(l.Line)),
        };
}
