using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Event;
using Akka.Hosting;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Akka.Serialization;
using Akka.Serialization.V2;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

// Its own namespace: two small entity types, orders and shipments, each with its own protocol and serializer.
namespace Akka.Durable.Tests.Hosting.Ownership;

using OrderContext = DecisionContext<OrderState, OrderCommand, OrderEvent>;
using ShipmentContext = DecisionContext<ShipmentState, ShipmentCommand, ShipmentEvent>;

// ---------------------------------------------------------------------------------------------------------------
// Orders
// ---------------------------------------------------------------------------------------------------------------

public interface IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-order-state-v1")]
public sealed record OrderState(
    [property: AkkaField(0)] bool Open,
    [property: AkkaField(1)] int Reserved,
    [property: AkkaField(2)] int Notified) : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-open-order-v1", AllowEmpty = true)]
public sealed record OpenOrder : IOrderProtocol, IReplies<OrderCommand, OrderAck>
{
    public OrderCommand ToCommand() => new(this);
}

/// <summary>Issues a <see cref="StockReservation"/>, whose handler answers with an <see cref="OrderCommand"/>.</summary>
[AkkaSerializable(Manifest = "ownership-reserve-stock-v1", AllowEmpty = true)]
public sealed record ReserveStock : IOrderProtocol;

/// <summary>The answer of <see cref="StockReservation"/>.</summary>
[AkkaSerializable(Manifest = "ownership-stock-reserved-v1", AllowEmpty = true)]
public sealed record StockReserved : IOrderProtocol;

/// <summary>Issues a <see cref="BuyerNotice"/>, whose handler has no answer.</summary>
[AkkaSerializable(Manifest = "ownership-notify-buyer-v1", AllowEmpty = true)]
public sealed record NotifyBuyer : IOrderProtocol;

/// <summary>Issues a <see cref="PlainBuyerNotice"/>: a no-answer effect with no serializer.</summary>
[AkkaSerializable(Manifest = "ownership-notify-buyer-plainly-v1", AllowEmpty = true)]
public sealed record NotifyBuyerPlainly : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-order-gave-up-v1")]
public sealed record OrderEffectGaveUp([property: AkkaField(0)] string What) : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-order-command-v1")]
public readonly union OrderCommand(OpenOrder, ReserveStock, StockReserved, NotifyBuyer, NotifyBuyerPlainly, OrderEffectGaveUp)
    : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-order-opened-v1", AllowEmpty = true)]
public sealed record OrderOpened : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-stock-requested-v1", AllowEmpty = true)]
public sealed record StockRequested : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-stock-confirmed-v1", AllowEmpty = true)]
public sealed record StockConfirmed : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-buyer-notified-v1", AllowEmpty = true)]
public sealed record BuyerNotified : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-order-event-v1")]
public readonly union OrderEvent(OrderOpened, StockRequested, StockConfirmed, BuyerNotified) : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-order-ack-v1", AllowEmpty = true)]
public sealed record OrderAck : IOrderProtocol;

// The effects of an order.
[AkkaSerializable(Manifest = "ownership-stock-reservation-v1")]
public sealed record StockReservation([property: AkkaField(0)] string Order) : IOrderProtocol;

[AkkaSerializable(Manifest = "ownership-buyer-notice-v1")]
public sealed record BuyerNotice([property: AkkaField(0)] string Order) : IOrderProtocol;

/// <summary>An effect of an order with no serializer: it carries no [AkkaSerializable].</summary>
public sealed record PlainBuyerNotice(string Order);

[AkkaSerializer<IOrderProtocol>("ownership-order-test", 22611)]
public sealed partial class OrderSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

// ---------------------------------------------------------------------------------------------------------------
// Shipments
// ---------------------------------------------------------------------------------------------------------------

public interface IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-shipment-state-v1")]
public sealed record ShipmentState(
    [property: AkkaField(0)] bool Created,
    [property: AkkaField(1)] int Booked,
    [property: AkkaField(2)] int Announced) : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-create-shipment-v1", AllowEmpty = true)]
public sealed record CreateShipment : IShipmentProtocol, IReplies<ShipmentCommand, ShipmentAck>
{
    public ShipmentCommand ToCommand() => new(this);
}

/// <summary>Issues a <see cref="CarrierBooking"/>, whose handler answers with a <see cref="ShipmentCommand"/>.</summary>
[AkkaSerializable(Manifest = "ownership-book-carrier-v1", AllowEmpty = true)]
public sealed record BookCarrier : IShipmentProtocol;

/// <summary>Issues a <see cref="PlainCarrierBooking"/>: an answering effect with no serializer.</summary>
[AkkaSerializable(Manifest = "ownership-book-carrier-plainly-v1", AllowEmpty = true)]
public sealed record BookCarrierPlainly : IShipmentProtocol;

/// <summary>The answer of <see cref="CarrierBooking"/> and of <see cref="PlainCarrierBooking"/>.</summary>
[AkkaSerializable(Manifest = "ownership-carrier-booked-v1", AllowEmpty = true)]
public sealed record CarrierBooked : IShipmentProtocol;

/// <summary>Issues a <see cref="DispatchNotice"/>, whose handler has no answer.</summary>
[AkkaSerializable(Manifest = "ownership-announce-dispatch-v1", AllowEmpty = true)]
public sealed record AnnounceDispatch : IShipmentProtocol;

/// <summary>Issues a <see cref="PlainDispatchNotice"/>: a no-answer effect with no serializer.</summary>
[AkkaSerializable(Manifest = "ownership-announce-dispatch-plainly-v1", AllowEmpty = true)]
public sealed record AnnounceDispatchPlainly : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-shipment-gave-up-v1")]
public sealed record ShipmentEffectGaveUp([property: AkkaField(0)] string What) : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-shipment-command-v1")]
public readonly union ShipmentCommand(
    CreateShipment, BookCarrier, BookCarrierPlainly, CarrierBooked, AnnounceDispatch, AnnounceDispatchPlainly,
    ShipmentEffectGaveUp) : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-shipment-created-v1", AllowEmpty = true)]
public sealed record ShipmentCreated : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-carrier-requested-v1", AllowEmpty = true)]
public sealed record CarrierRequested : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-carrier-confirmed-v1", AllowEmpty = true)]
public sealed record CarrierConfirmed : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-dispatch-announced-v1", AllowEmpty = true)]
public sealed record DispatchAnnounced : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-shipment-event-v1")]
public readonly union ShipmentEvent(ShipmentCreated, CarrierRequested, CarrierConfirmed, DispatchAnnounced)
    : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-shipment-ack-v1", AllowEmpty = true)]
public sealed record ShipmentAck : IShipmentProtocol;

// The effects of a shipment.
[AkkaSerializable(Manifest = "ownership-carrier-booking-v1")]
public sealed record CarrierBooking([property: AkkaField(0)] string Shipment) : IShipmentProtocol;

[AkkaSerializable(Manifest = "ownership-dispatch-notice-v1")]
public sealed record DispatchNotice([property: AkkaField(0)] string Shipment) : IShipmentProtocol;

/// <summary>An answering effect of a shipment with no serializer.</summary>
public sealed record PlainCarrierBooking(string Shipment);

/// <summary>A no-answer effect of a shipment with no serializer.</summary>
public sealed record PlainDispatchNotice(string Shipment);

[AkkaSerializer<IShipmentProtocol>("ownership-shipment-test", 22612)]
public sealed partial class ShipmentSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

// ---------------------------------------------------------------------------------------------------------------
// Handlers and definitions
// ---------------------------------------------------------------------------------------------------------------

/// <summary>Which handlers ran. Each handler completes the task of its own name.</summary>
public sealed class HandlerCalls
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _calls = new();

    private TaskCompletionSource Of(string name) =>
        _calls.GetOrAdd(name, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

    public void Called(string name) => Of(name).TrySetResult();

    public Task WhenCalled(string name) => Of(name).Task;

    public bool WasCalled(string name) => _calls.TryGetValue(name, out var call) && call.Task.IsCompleted;
}

public sealed class StockReservationHandler(HandlerCalls calls) : IEffectHandler<StockReservation, OrderCommand>
{
    public Task<OrderCommand> HandleAsync(StockReservation effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        calls.Called(nameof(StockReservation));
        return Task.FromResult<OrderCommand>(new StockReserved());
    }
}

public sealed class BuyerNoticeHandler(HandlerCalls calls) : IEffectHandler<BuyerNotice>
{
    public Task HandleAsync(BuyerNotice effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        calls.Called(nameof(BuyerNotice));
        return Task.CompletedTask;
    }
}

public sealed class PlainBuyerNoticeHandler(HandlerCalls calls) : IEffectHandler<PlainBuyerNotice>
{
    public Task HandleAsync(PlainBuyerNotice effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        calls.Called(nameof(PlainBuyerNotice));
        return Task.CompletedTask;
    }
}

public sealed class CarrierBookingHandler(HandlerCalls calls) : IEffectHandler<CarrierBooking, ShipmentCommand>
{
    public Task<ShipmentCommand> HandleAsync(CarrierBooking effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        calls.Called(nameof(CarrierBooking));
        return Task.FromResult<ShipmentCommand>(new CarrierBooked());
    }
}

public sealed class PlainCarrierBookingHandler(HandlerCalls calls) : IEffectHandler<PlainCarrierBooking, ShipmentCommand>
{
    public Task<ShipmentCommand> HandleAsync(PlainCarrierBooking effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        calls.Called(nameof(PlainCarrierBooking));
        return Task.FromResult<ShipmentCommand>(new CarrierBooked());
    }
}

public sealed class DispatchNoticeHandler(HandlerCalls calls) : IEffectHandler<DispatchNotice>
{
    public Task HandleAsync(DispatchNotice effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        calls.Called(nameof(DispatchNotice));
        return Task.CompletedTask;
    }
}

public sealed class PlainDispatchNoticeHandler(HandlerCalls calls) : IEffectHandler<PlainDispatchNotice>
{
    public Task HandleAsync(PlainDispatchNotice effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        calls.Called(nameof(PlainDispatchNotice));
        return Task.CompletedTask;
    }
}

public static class Orders
{
    public static readonly EntityDefinition<OrderState, OrderCommand, OrderEvent> Definition = new(
        new EntityTypeName("orders"),
        _ => new OrderState(false, 0, 0),
        Decide,
        Evolve,
        GaveUp: g => new OrderEffectGaveUp($"{g.Work.GetType().Name}:{g.Reason}"));

    private static Decision<OrderCommand, OrderEvent> Decide(OrderState state, OrderCommand command, OrderContext ctx) =>
        command switch
        {
            OpenOrder c => ctx.Persist(new OrderOpened()).Reply(c, new OrderAck()),
            ReserveStock => ctx.Persist(new StockRequested()).Execute(new StockReservation(ctx.EntityId.Value)),
            StockReserved => ctx.Persist(new StockConfirmed()),
            NotifyBuyer => ctx.Persist(new BuyerNotified()).Execute(new BuyerNotice(ctx.EntityId.Value)),
            NotifyBuyerPlainly => ctx.Persist(new BuyerNotified()).Execute(new PlainBuyerNotice(ctx.EntityId.Value)),
            OrderEffectGaveUp => ctx.Ignore(),
        };

    private static OrderState Evolve(OrderState state, OrderEvent @event) =>
        @event switch
        {
            OrderOpened => state with { Open = true },
            StockConfirmed => state with { Reserved = state.Reserved + 1 },
            BuyerNotified => state with { Notified = state.Notified + 1 },
            _ => state,
        };
}

public static class Shipments
{
    public static readonly EntityDefinition<ShipmentState, ShipmentCommand, ShipmentEvent> Definition = new(
        new EntityTypeName("shipments"),
        _ => new ShipmentState(false, 0, 0),
        Decide,
        Evolve,
        GaveUp: g => new ShipmentEffectGaveUp($"{g.Work.GetType().Name}:{g.Reason}"));

    private static Decision<ShipmentCommand, ShipmentEvent> Decide(
        ShipmentState state, ShipmentCommand command, ShipmentContext ctx) =>
        command switch
        {
            CreateShipment c => ctx.Persist(new ShipmentCreated()).Reply(c, new ShipmentAck()),
            BookCarrier => ctx.Persist(new CarrierRequested()).Execute(new CarrierBooking(ctx.EntityId.Value)),
            BookCarrierPlainly => ctx.Persist(new CarrierRequested()).Execute(new PlainCarrierBooking(ctx.EntityId.Value)),
            CarrierBooked => ctx.Persist(new CarrierConfirmed()),
            AnnounceDispatch => ctx.Persist(new DispatchAnnounced()).Execute(new DispatchNotice(ctx.EntityId.Value)),
            AnnounceDispatchPlainly =>
                ctx.Persist(new DispatchAnnounced()).Execute(new PlainDispatchNotice(ctx.EntityId.Value)),
            ShipmentEffectGaveUp => ctx.Ignore(),
        };

    private static ShipmentState Evolve(ShipmentState state, ShipmentEvent @event) =>
        @event switch
        {
            ShipmentCreated => state with { Created = true },
            CarrierConfirmed => state with { Booked = state.Booked + 1 },
            DispatchAnnounced => state with { Announced = state.Announced + 1 },
            _ => state,
        };
}

// ---------------------------------------------------------------------------------------------------------------
// Specs
// ---------------------------------------------------------------------------------------------------------------

/// <summary>
/// An effect handler with no answer does not say which entity type issues its effect. Its effect is the concern of
/// the entity type that issues it, and of no other: a host that runs only orders starts when the container also holds
/// the handlers of shipments, whose serializer it does not register. An entity type whose own effect has no serializer
/// is still refused, by its own name, before anything is written.
/// </summary>
public sealed class EffectRouteOwnershipSpecs
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    private static CancellationToken AskCancellation()
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(Patience);
        return cts.Token;
    }

    private static IServiceCollection OrderHandlers(IServiceCollection services) => services
        .AddEffectHandler<StockReservation, OrderCommand, StockReservationHandler>()
        .AddEffectHandler<BuyerNotice, BuyerNoticeHandler>();

    private static IServiceCollection ShipmentHandlers(IServiceCollection services) => services
        .AddEffectHandler<CarrierBooking, ShipmentCommand, CarrierBookingHandler>()
        .AddEffectHandler<DispatchNotice, DispatchNoticeHandler>();

    private static AkkaConfigurationBuilder HostOrders(AkkaConfigurationBuilder builder) =>
        builder.WithCustomSerializer(OrderSerializer.CreateSerializerDetails).WithDurableEntity(Orders.Definition);

    private static AkkaConfigurationBuilder HostShipments(AkkaConfigurationBuilder builder) =>
        builder.WithCustomSerializer(ShipmentSerializer.CreateSerializerDetails).WithDurableEntity(Shipments.Definition);

    private static IHost BuildHost(Action<IServiceCollection> handlers, Action<AkkaConfigurationBuilder> akka) =>
        new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddSingleton<HandlerCalls>();
                handlers(services);
                services.AddAkka("effect-route-ownership", builder =>
                {
                    builder.WithLocalReminders(reminders => reminders
                        .WithStorage(_ => new InMemoryReminderStorage())
                        .WithResolver(system => new DurableEntityReminderResolver(system)));
                    akka(builder);
                });
            })
            .Build();

    private static IDurableEntityRef<OrderCommand, OrderState, OrderEvent> OrderRef(IHost host, string id) =>
        host.Services.GetRequiredService<IDurableEntityRefs<OrderCommand, OrderState, OrderEvent>>().Get(new EntityId(id));

    private static IDurableEntityRef<ShipmentCommand, ShipmentState, ShipmentEvent> ShipmentRef(IHost host, string id) =>
        host.Services.GetRequiredService<IDurableEntityRefs<ShipmentCommand, ShipmentState, ShipmentEvent>>()
            .Get(new EntityId(id));

    private static HandlerCalls Calls(IHost host) => host.Services.GetRequiredService<HandlerCalls>();

    private static async Task Started(IHost host)
    {
        try
        {
            await host.StartAsync(Ct);
        }
        catch (Exception failure)
        {
            Assert.Fail($"The host did not start: {string.Join(" | ", StartupProbe.Messages(failure))}");
        }
    }

    /// <summary>Both kinds of order effect run: the answer reaches Decide, and the no-answer handler is called.</summary>
    private static async Task AssertOrderEffectsRun(IHost host)
    {
        var order = OrderRef(host, "o-1");
        await order.Ask(new OpenOrder(), AskCancellation());

        order.Tell(new ReserveStock());
        order.Tell(new NotifyBuyer());

        await Calls(host).WhenCalled(nameof(StockReservation)).WaitAsync(Patience, Ct);
        await Calls(host).WhenCalled(nameof(BuyerNotice)).WaitAsync(Patience, Ct);
        await order.WaitForStateAsync(s => s.Reserved == 1, AskCancellation());
    }

    private static async Task AssertShipmentEffectsRun(IHost host)
    {
        var shipment = ShipmentRef(host, "s-1");
        await shipment.Ask(new CreateShipment(), AskCancellation());

        shipment.Tell(new BookCarrier());
        shipment.Tell(new AnnounceDispatch());

        await Calls(host).WhenCalled(nameof(CarrierBooking)).WaitAsync(Patience, Ct);
        await Calls(host).WhenCalled(nameof(DispatchNotice)).WaitAsync(Patience, Ct);
        await shipment.WaitForStateAsync(s => s.Booked == 1, AskCancellation());
    }

    /// <summary>What a refusal said, and where it happened.</summary>
    private sealed record Refusal(string Text, bool AtStart);

    /// <summary>Completes when an error is logged whose text contains the effect's name.</summary>
    private sealed class RefusalSink : ReceiveActor
    {
        public RefusalSink(string effect, TaskCompletionSource<string> found)
        {
            Receive<LogEvent>(e =>
            {
                var text = $"{e.Message} {e.Cause}";
                if (e is Error && text.Contains(effect, StringComparison.Ordinal)) found.TrySetResult(text);
            });
        }
    }

    /// <summary>
    /// Starts the host. When the start is refused, returns its failure. When it starts, makes one write through
    /// <paramref name="first"/>, then issues the effect through <paramref name="issue"/>, and expects that Execute to be
    /// refused before the write: an error naming the effect is logged, nothing more is journaled, and the handler of
    /// the effect is never called.
    /// </summary>
    private static async Task<Refusal> RefusedBeforeAnyWrite(
        Action<IServiceCollection> handlers,
        Action<AkkaConfigurationBuilder> akka,
        string effect,
        Func<IHost, Task<long>> first,
        Action<IHost> issue,
        Func<IHost, Task<long>> sequenceNr)
    {
        using var host = BuildHost(handlers, akka);
        try
        {
            await host.StartAsync(Ct);
        }
        catch (Exception failure)
        {
            await host.StopAsync(Ct);
            return new Refusal(string.Join(" | ", StartupProbe.Messages(failure)), AtStart: true);
        }

        try
        {
            var before = await first(host);

            var system = host.Services.GetRequiredService<ActorSystem>();
            var found = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var sink = system.ActorOf(Props.Create(() => new RefusalSink(effect, found)));
            system.EventStream.Subscribe(sink, typeof(Error));

            issue(host);

            string text;
            try
            {
                text = await found.Task.WaitAsync(Patience, Ct);
            }
            catch (TimeoutException)
            {
                Assert.Fail($"The host started, and the first Execute of {effect} was not refused within {Patience}.");
                throw;
            }

            Assert.Equal(before, await sequenceNr(host)); // the refused decision wrote nothing
            Assert.False(Calls(host).WasCalled(effect), $"The handler of {effect} was called.");
            return new Refusal(text, AtStart: false);
        }
        finally
        {
            await host.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_host_with_both_entity_types_and_both_serializers_starts_and_runs_the_effects_of_each()
    {
        using var host = BuildHost(
            services =>
            {
                OrderHandlers(services);
                ShipmentHandlers(services);
            },
            builder => HostShipments(HostOrders(builder)));

        await Started(host);
        try
        {
            await AssertOrderEffectsRun(host);
            await AssertShipmentEffectsRun(host);
        }
        finally
        {
            await host.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_host_of_orders_alone_starts_while_the_container_holds_the_handlers_of_shipments_without_their_serializer()
    {
        // The shipments' handlers are in the container, as in an application whose other hosts run shipments. This
        // host runs orders only and does not register the shipments' serializer.
        using var host = BuildHost(
            services =>
            {
                ShipmentHandlers(services);
                OrderHandlers(services);
            },
            builder => HostOrders(builder));

        await Started(host);
        try
        {
            await AssertOrderEffectsRun(host);
        }
        finally
        {
            await host.StopAsync(Ct);
        }
    }

    [Fact]
    public async Task A_shipment_effect_with_an_answer_and_no_serializer_refuses_the_start_and_names_shipments_and_the_effect()
    {
        using var host = BuildHost(
            services => services
                .AddEffectHandler<PlainCarrierBooking, ShipmentCommand, PlainCarrierBookingHandler>()
                .AddEffectHandler<DispatchNotice, DispatchNoticeHandler>(),
            builder => HostShipments(builder));

        Exception? failure = null;
        try
        {
            await host.StartAsync(Ct);
        }
        catch (Exception refused)
        {
            failure = refused;
        }
        finally
        {
            await host.StopAsync(Ct);
        }

        Assert.NotNull(failure);
        Assert.Contains(StartupProbe.Messages(failure), message => message.StartsWith(
            "Durable entity [shipments] cannot start: the effect PlainCarrierBooking, handled by PlainCarrierBookingHandler",
            StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_shipment_effect_with_no_answer_and_no_serializer_is_refused_before_any_write_naming_shipments_and_the_effect()
    {
        var refusal = await RefusedBeforeAnyWrite(
            services => services
                .AddEffectHandler<CarrierBooking, ShipmentCommand, CarrierBookingHandler>()
                .AddEffectHandler<PlainDispatchNotice, PlainDispatchNoticeHandler>(),
            builder => HostShipments(builder),
            nameof(PlainDispatchNotice),
            first: async host =>
            {
                await ShipmentRef(host, "s-4").Ask(new CreateShipment(), AskCancellation());
                return (await ShipmentRef(host, "s-4").GetStateAsync(AskCancellation())).SequenceNr;
            },
            issue: host => ShipmentRef(host, "s-4").Tell(new AnnounceDispatchPlainly()),
            sequenceNr: async host => (await ShipmentRef(host, "s-4").GetStateAsync(AskCancellation())).SequenceNr);

        Assert.Contains("[shipments]", refusal.Text);
        Assert.Contains(nameof(PlainDispatchNotice), refusal.Text);
    }

    [Fact]
    public async Task An_order_effect_with_no_answer_and_no_serializer_is_refused_naming_orders_and_not_shipments()
    {
        // As in the host of orders alone, and the order's own no-answer effect has no serializer. The shipments'
        // handlers are registered first.
        var refusal = await RefusedBeforeAnyWrite(
            services =>
            {
                ShipmentHandlers(services);
                services
                    .AddEffectHandler<StockReservation, OrderCommand, StockReservationHandler>()
                    .AddEffectHandler<PlainBuyerNotice, PlainBuyerNoticeHandler>();
            },
            builder => HostOrders(builder),
            nameof(PlainBuyerNotice),
            first: async host =>
            {
                await OrderRef(host, "o-5").Ask(new OpenOrder(), AskCancellation());
                return (await OrderRef(host, "o-5").GetStateAsync(AskCancellation())).SequenceNr;
            },
            issue: host => OrderRef(host, "o-5").Tell(new NotifyBuyerPlainly()),
            sequenceNr: async host => (await OrderRef(host, "o-5").GetStateAsync(AskCancellation())).SequenceNr);

        Assert.Contains("[orders]", refusal.Text);
        Assert.Contains(nameof(PlainBuyerNotice), refusal.Text);
        Assert.DoesNotContain("[shipments]", refusal.Text);
        Assert.DoesNotContain("the effect " + nameof(DispatchNotice), refusal.Text);
    }
}
