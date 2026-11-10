using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;
using Microsoft.Extensions.DependencyInjection;

// Its own namespace: names such as Reserved and Charged are too general to share with other fixtures.
namespace Akka.Durable.Tests.Fixtures.Checkouts;

// A checkout workflow: reserve stock, charge the card, and undo the order when the charge is declined.
// Work that gets no answer sends the workflow to NeedsAttention, which opens a support ticket and waits.

public interface ICheckoutProtocol;

// Input
[AkkaSerializable(Manifest = "checkout-place-order-v1")]
public sealed record PlaceOrder(
    [property: AkkaField(0)] string Order,
    [property: AkkaField(1)] int Total) : ICheckoutProtocol;

// States
[AkkaSerializable(Manifest = "checkout-reserving-v1", AllowEmpty = true)]
public sealed record Reserving : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-charging-v1")]
public sealed record Charging([property: AkkaField(0)] string Reservation) : ICheckoutProtocol;

/// <summary>Compensation, then end with <see cref="Then"/>: a state that holds an outcome.</summary>
[AkkaSerializable(Manifest = "checkout-undoing-v1")]
public sealed record Undoing([property: AkkaField(0)] CheckoutOutcome Then) : ICheckoutProtocol;

/// <summary>Waiting for a person. Holds the state it came from: the state union inside one of its own cases.</summary>
[AkkaSerializable(Manifest = "checkout-needs-attention-v1")]
public sealed record NeedsAttention(
    [property: AkkaField(0)] CheckoutState Was,
    [property: AkkaField(1)] string Why) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-state-v1")]
public readonly union CheckoutState(Reserving, Charging, Undoing, NeedsAttention) : ICheckoutProtocol;

// Facts
[AkkaSerializable(Manifest = "checkout-reserved-v1")]
public sealed record Reserved([property: AkkaField(0)] string Reservation) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-out-of-stock-v1", AllowEmpty = true)]
public sealed record OutOfStock : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-charged-v1")]
public sealed record Charged([property: AkkaField(0)] string Charge) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-declined-v1")]
public sealed record Declined([property: AkkaField(0)] string Reason) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-undone-v1", AllowEmpty = true)]
public sealed record Undone : ICheckoutProtocol;

/// <summary>The undo did not go through this time. The table issues it again.</summary>
[AkkaSerializable(Manifest = "checkout-undo-refused-v1", AllowEmpty = true)]
public sealed record UndoRefused : ICheckoutProtocol;

/// <summary>A piece of work got no answer.</summary>
[AkkaSerializable(Manifest = "checkout-work-stuck-v1")]
public sealed record WorkStuck(
    [property: AkkaField(0)] string What,
    [property: AkkaField(1)] GaveUpReason Reason) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-ticket-opened-v1", AllowEmpty = true)]
public sealed record TicketOpened : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-fact-v1")]
public readonly union CheckoutFact(
    Reserved, OutOfStock, Charged, Declined, Undone, UndoRefused, WorkStuck, TicketOpened) : ICheckoutProtocol;

// Outcomes
[AkkaSerializable(Manifest = "checkout-order-placed-v1")]
public sealed record OrderPlaced([property: AkkaField(0)] string Charge) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-order-failed-v1")]
public sealed record OrderFailed([property: AkkaField(0)] string Reason) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-outcome-v1")]
public readonly union CheckoutOutcome(OrderPlaced, OrderFailed) : ICheckoutProtocol;

// Effects. Plain records: they never leave the process.
public sealed record ReserveStock(string Order);
public sealed record ChargeCard(string Order, int Total);
public sealed record UndoOrder(string Order);
public sealed record OpenSupportTicket(string Checkout, string Why);

[AkkaSerializer<ICheckoutProtocol>("checkout-test", 22601)]
public sealed partial class CheckoutSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Checkout
{
    public static readonly WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =
        new(new WorkflowTypeName("checkouts"),
            Start: _ => new Reserving(),
            Work,
            Decide,
            Kind,
            GaveUp: failure => new WorkStuck(failure.What, failure.Reason));

    /// <summary>The work each state needs. Every effect is built from the state and the input alone.</summary>
    public static WorkList<CheckoutFact> Work(CheckoutState state, WorkContext<PlaceOrder, CheckoutFact> ctx) =>
        state switch
        {
            Reserving => ctx.Execute(new ReserveStock(ctx.Input.Order)),
            Charging => ctx.Execute(new ChargeCard(ctx.Input.Order, ctx.Input.Total)),
            Undoing => ctx.Execute(new UndoOrder(ctx.Input.Order)),
            NeedsAttention attention => ctx.Execute(new OpenSupportTicket(ctx.Id.Value, attention.Why)),
        };

    /// <summary>
    /// The fact table. It has no catch-all arm: it compiles only because every pair of state and fact is
    /// covered, and this project treats warnings as errors.
    /// </summary>
    public static Transition<CheckoutState, CheckoutOutcome> Decide(
        CheckoutState state, CheckoutFact fact, DecideContext<PlaceOrder, CheckoutState, CheckoutOutcome> ctx) =>
        (state, fact) switch
        {
            (Reserving, Reserved reserved) => ctx.Next(new Charging(reserved.Reservation)),
            (Reserving, OutOfStock) => ctx.Finish(new OrderFailed("out of stock")),

            (Charging, Charged charged) => ctx.Finish(new OrderPlaced(charged.Charge)),
            (Charging, Declined declined) => ctx.Next(new Undoing(new OrderFailed(declined.Reason))),

            (Undoing undoing, Undone) => ctx.Finish(undoing.Then),
            (Undoing, UndoRefused) => ctx.Again(),

            // Work that got no answer: a person decides. The state it came from is kept.
            (Reserving or Charging or Undoing, WorkStuck stuck) =>
                ctx.Next(new NeedsAttention(state, $"{stuck.What} {stuck.Reason}")),
            (NeedsAttention, TicketOpened) => ctx.Stay(),
            (NeedsAttention, WorkStuck) => ctx.Stay(),

            // Pairs that should not happen, written out so that a new state or fact does not compile until it is placed.
            (Reserving, Charged or Declined or Undone or UndoRefused or TicketOpened) => ctx.Unexpected(),
            (Charging, Reserved or OutOfStock or Undone or UndoRefused or TicketOpened) => ctx.Unexpected(),
            (Undoing, Reserved or OutOfStock or Charged or Declined or TicketOpened) => ctx.Unexpected(),
            (NeedsAttention, Reserved or OutOfStock or Charged or Declined or Undone or UndoRefused) => ctx.Unexpected(),
        };

    public static OutcomeKind Kind(CheckoutOutcome outcome) =>
        outcome switch
        {
            OrderPlaced => OutcomeKind.Succeeded,
            OrderFailed => OutcomeKind.Failed,
        };
}

/// <summary>
/// What the checkout's services do in a test, and what they were asked to do. Each handler records the
/// effect it received and then answers the way the test set it up.
/// </summary>
public sealed class CheckoutServices
{
    private readonly ConcurrentQueue<object> _issued = new();

    /// <summary>Every effect a handler received, in order.</summary>
    public IReadOnlyList<object> Issued => [.. _issued];

    public Func<ReserveStock, CancellationToken, Task<CheckoutFact>> Reserve { get; set; } =
        (effect, _) => Task.FromResult<CheckoutFact>(new Reserved($"res-{effect.Order}"));

    public Func<ChargeCard, CancellationToken, Task<CheckoutFact>> Charge { get; set; } =
        (effect, _) => Task.FromResult<CheckoutFact>(new Charged($"charge-{effect.Order}"));

    public Func<UndoOrder, CancellationToken, Task<CheckoutFact>> Undo { get; set; } =
        (_, _) => Task.FromResult<CheckoutFact>(new Undone());

    public Func<OpenSupportTicket, CancellationToken, Task<CheckoutFact>> OpenTicket { get; set; } =
        (_, _) => Task.FromResult<CheckoutFact>(new TicketOpened());

    internal Task<CheckoutFact> Run<TEffect>(
        TEffect effect, Func<TEffect, CancellationToken, Task<CheckoutFact>> answer, CancellationToken ct)
        where TEffect : notnull
    {
        _issued.Enqueue(effect);
        return answer(effect, ct);
    }
}

public sealed class ReserveStockHandler(CheckoutServices services) : IEffectHandler<ReserveStock, CheckoutFact>
{
    public Task<CheckoutFact> HandleAsync(ReserveStock effect, CancellationToken cancellationToken) =>
        services.Run(effect, services.Reserve, cancellationToken);
}

public sealed class ChargeCardHandler(CheckoutServices services) : IEffectHandler<ChargeCard, CheckoutFact>
{
    public Task<CheckoutFact> HandleAsync(ChargeCard effect, CancellationToken cancellationToken) =>
        services.Run(effect, services.Charge, cancellationToken);
}

public sealed class UndoOrderHandler(CheckoutServices services) : IEffectHandler<UndoOrder, CheckoutFact>
{
    public Task<CheckoutFact> HandleAsync(UndoOrder effect, CancellationToken cancellationToken) =>
        services.Run(effect, services.Undo, cancellationToken);
}

public sealed class OpenSupportTicketHandler(CheckoutServices services) : IEffectHandler<OpenSupportTicket, CheckoutFact>
{
    public Task<CheckoutFact> HandleAsync(OpenSupportTicket effect, CancellationToken cancellationToken) =>
        services.Run(effect, services.OpenTicket, cancellationToken);
}

public static class CheckoutHandlers
{
    public static IServiceCollection AddCheckoutHandlers(this IServiceCollection services) => services
        .AddSingleton<CheckoutServices>()
        .AddScoped<IEffectHandler<ReserveStock, CheckoutFact>, ReserveStockHandler>()
        .AddScoped<IEffectHandler<ChargeCard, CheckoutFact>, ChargeCardHandler>()
        .AddScoped<IEffectHandler<UndoOrder, CheckoutFact>, UndoOrderHandler>()
        .AddScoped<IEffectHandler<OpenSupportTicket, CheckoutFact>, OpenSupportTicketHandler>();
}
