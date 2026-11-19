using Akka.Durable.Streams;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace Akka.Durable.Tests.Docs;

// The code samples of the stream pages, in README.md and in docs/streams.md. They are here so that they compile:
// a change to the API that breaks a sample breaks this file. Nothing runs them. The text between a pair of
// "sample" lines is what the page shows, so keep the two the same. The method around a sample, and the types
// below, are the setting the page describes in words.

/// <summary>A message from a queue.</summary>
public sealed record OrderMessage(string Order, int Total);

/// <summary>A change of price for one product.</summary>
public sealed record PriceChange(string Sku, int Cents);

public sealed record ChangePrice(int Cents) : IReplies<ProductCommand, PriceChanged>
{
    public ProductCommand ToCommand() => new(this);
}

public sealed record PriceChanged(int OldCents, int NewCents);

public readonly union ProductCommand(ChangePrice);

/// <summary>A shipment that needs a label.</summary>
public sealed record Shipment(string Number);

public sealed record PrintLabel(string Shipment);

public sealed record LabelOutcome(string File);

/// <summary>A request from a support tool to try an order again.</summary>
public sealed record RetryRequest(string Order);

public sealed record Open;
public sealed record Packed(string Carrier);
public sealed record Arrived(string Carrier, string Tracking);
public readonly union OrderState(Open, Packed, Arrived);

/// <summary>What a page shows about one order.</summary>
public sealed record OrderView(string Status, string Detail)
{
    public bool IsDelivered => Status == "Delivered";
}

public static class StreamDocSamples
{
    // The checkout workflow is the one from the Workflows page.
    public static Task StartPerOrder(
        Source<OrderMessage, NotUsed> orders,
        IWorkflowRefs<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> checkouts,
        Action<OrderMessage> retry,
        IMaterializer materializer)
    {
        // <sample readme-start>
        orders
            .Via(DurableFlow.Start(
                checkouts,
                (OrderMessage m) => (new WorkflowId(m.Order), new PlaceOrder(m.Order, m.Total)),
                parallelism: 8,
                timeout: TimeSpan.FromSeconds(5)))
            .RunForeach(r =>
            {
                // No answer in time does not mean the start failed. The id comes from the order,
                // so sending the order through again reaches the same workflow.
                if (r.Answer is TimedOut or Failed)
                    retry(r.Input);
            }, materializer);
        // </sample>

        return Task.CompletedTask;
    }

    public static Task<Done> StartThenCommit(
        Source<OrderMessage, NotUsed> orders,
        IWorkflowRefs<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> checkouts,
        Sink<(OrderMessage Input, Answer<StartResult<CheckoutOutcome>> Answer), NotUsed> park,
        Sink<OrderMessage, Task<Done>> commit,
        IMaterializer materializer)
    {
        // <sample start>
        return orders
            .Via(DurableFlow.Start(
                checkouts,
                (OrderMessage m) => (new WorkflowId(m.Order), new PlaceOrder(m.Order, m.Total)),
                parallelism: 8,
                timeout: TimeSpan.FromSeconds(5)))
            // A stored start is a value: Started, AlreadyRunning or AlreadyFinished.
            // Only a timeout or an exception means we do not know.
            .DivertTo(park, r => r.Answer is TimedOut or Failed)
            .Select(r => r.Input)
            .RunWith(commit, materializer);
        // </sample>
    }

    public static Task ReportPriceChanges(
        Source<PriceChange, NotUsed> changes,
        IEntityRefs<ProductCommand> products,
        Action<string> report,
        IMaterializer materializer)
    {
        // <sample ask>
        return changes
            .Via(DurableFlow.Ask(
                products,
                (PriceChange c) => (new EntityId(c.Sku), new ChangePrice(c.Cents)),
                parallelism: 8,
                timeout: TimeSpan.FromSeconds(10)))
            .Select(r => r.Answer switch
            {
                PriceChanged done => $"{r.Input.Sku}: {done.OldCents} to {done.NewCents}",
                TimedOut => $"{r.Input.Sku}: no answer in time. Check it before you send it again.",
                Failed failed => $"{r.Input.Sku}: failed, {failed.Error.Message}",
            })
            .RunForeach(report, materializer);
        // </sample>
    }

    public static Task NotifyRetries(
        Source<RetryRequest, NotUsed> requests,
        IWorkflowRefs<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> checkouts,
        Action<string> report,
        IMaterializer materializer)
    {
        // <sample notify>
        return requests
            .Via(DurableFlow.Notify(
                checkouts,
                (RetryRequest r) => (new WorkflowId(r.Order), new OperatorSaidRetry()),
                parallelism: 4,
                timeout: TimeSpan.FromSeconds(5)))
            .Select(r => r.Answer switch
            {
                // The answer is itself a union, so a switch names it first.
                DeliveryResult<CheckoutOutcome> d => d switch
                {
                    Delivered => $"{r.Input.Order}: taken",
                    Held => $"{r.Input.Order}: taken, decided when the work in flight ends",
                    WorkflowNotStarted => $"{r.Input.Order}: no such checkout",
                    AlreadyFinished<CheckoutOutcome> => $"{r.Input.Order}: already finished",
                },
                TimedOut => $"{r.Input.Order}: no answer in time",
                Failed failed => $"{r.Input.Order}: failed, {failed.Error.Message}",
            })
            .RunForeach(report, materializer);
        // </sample>
    }

    public static Task<Done> PrintLabels(
        Source<Shipment, NotUsed> shipments,
        IWorkflowRefs<PrintLabel, string, string, LabelOutcome> labels,
        Sink<(Shipment Input, Answer<StateSnapshot<WorkflowStatus<string, LabelOutcome>>> Answer), NotUsed> followUp,
        Sink<Shipment, Task<Done>> attachToParcel,
        IMaterializer materializer)
    {
        // <sample until>
        return shipments
            .Via(DurableFlow.Start(
                labels,
                (Shipment s) => (new WorkflowId($"label-{s.Number}"), new PrintLabel(s.Number)),
                parallelism: 8,
                timeout: TimeSpan.FromSeconds(5)))
            // A start that got no answer may still be stored, so look for the workflow anyway.
            .Select(r => r.Input)
            .Via(DurableFlow.Until(
                labels,
                (Shipment s) => new WorkflowId($"label-{s.Number}"),
                (s, status) => status is WorkflowFinished<LabelOutcome>,
                parallelism: 8,
                timeout: TimeSpan.FromSeconds(30)))
            // The label is not done in 30 seconds, or the wait failed: leave it to the follow-up.
            .DivertTo(followUp, r => r.Answer is TimedOut or Failed)
            .Select(r => r.Input)
            .RunWith(attachToParcel, materializer);
        // </sample>
    }

    public static Task ShowOrder(
        IDurableEntityRef<string, OrderState, string> order,
        Action<OrderView> publish,
        IMaterializer materializer)
    {
        // <sample watch>
        return DurableSource.Watch(order)
            .Select(snapshot => snapshot.State switch
            {
                Open => new OrderView("Open", "Waiting for the warehouse"),
                Packed p => new OrderView("Packed", $"Handed to {p.Carrier}"),
                Arrived a => new OrderView("Delivered", $"Tracking {a.Tracking}"),
            })
            // Watching an entity never ends by itself. Stop at the last state you care about.
            .TakeWhile(view => !view.IsDelivered, inclusive: true)
            .RunForeach(publish, materializer);
        // </sample>
    }
}
