using System.Collections.Immutable;
using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

// Its own namespace: the names here (Running, Finished, Next, Stay) are too general to share with other fixtures.
namespace Akka.Durable.Tests.Fixtures.WorkflowProbe;

// Probe for the workflow layer: can wrapper types owned by the library carry an author's own types through
// serialization? Nothing here is the workflow engine. The author's part is what a workflow author would
// write; the wrapper part stands in for types the library would own, and is to be replaced by them.

// ---------------------------------------------------------------------------------------------------
// The author's part: an input, a state union, a fact union, an outcome union, and one serializer.
// ---------------------------------------------------------------------------------------------------

public interface ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-input-v1")]
public sealed record CheckoutInput(
    [property: AkkaField(0)] string Order,
    [property: AkkaField(1)] int Total) : ICheckoutProtocol;

// States
[AkkaSerializable(Manifest = "checkout-reserving-v1", AllowEmpty = true)]
public sealed record Reserving : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-charging-v1")]
public sealed record Charging([property: AkkaField(0)] string Reservation) : ICheckoutProtocol;

/// <summary>A state that holds the state it came from: the state union inside one of its own cases.</summary>
[AkkaSerializable(Manifest = "checkout-needs-attention-v1")]
public sealed record NeedsAttention(
    [property: AkkaField(0)] CheckoutState Was,
    [property: AkkaField(1)] string Why) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-state-v1")]
public readonly union CheckoutState(Reserving, Charging, NeedsAttention) : ICheckoutProtocol;

// Facts
[AkkaSerializable(Manifest = "checkout-reserved-v1")]
public sealed record Reserved([property: AkkaField(0)] string Reservation) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-approved-v1")]
public sealed record Approved([property: AkkaField(0)] string Charge) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-refused-v1")]
public sealed record Refused([property: AkkaField(0)] string Why) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-payment-reply-v1")]
public readonly union PaymentReply(Approved, Refused) : ICheckoutProtocol;

/// <summary>A fact that holds another union.</summary>
[AkkaSerializable(Manifest = "checkout-payment-answered-v1")]
public sealed record PaymentAnswered([property: AkkaField(0)] PaymentReply Reply) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-work-stuck-v1")]
public sealed record WorkStuck([property: AkkaField(0)] string What) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-operator-said-retry-v1", AllowEmpty = true)]
public sealed record OperatorSaidRetry : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-fact-v1")]
public readonly union CheckoutFact(Reserved, PaymentAnswered, WorkStuck, OperatorSaidRetry) : ICheckoutProtocol;

// Outcomes
[AkkaSerializable(Manifest = "checkout-order-placed-v1")]
public sealed record OrderPlaced([property: AkkaField(0)] string Charge) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-order-failed-v1")]
public sealed record OrderFailed([property: AkkaField(0)] string Why) : ICheckoutProtocol;

[AkkaSerializable(Manifest = "checkout-outcome-v1")]
public readonly union CheckoutOutcome(OrderPlaced, OrderFailed) : ICheckoutProtocol;

[AkkaSerializer<ICheckoutProtocol>("checkout-probe", 22601)]
public sealed partial class CheckoutSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Checkout
{
    public static readonly EntityTypeName TypeName = new("checkout-probe");

    public static CheckoutState Start(CheckoutInput input) => new Reserving();

    /// <summary>
    /// The fact table. It has no catch-all arm, so it compiles only because the compiler can prove every
    /// (state, fact) pair is covered: this project treats warnings as errors.
    /// </summary>
    public static Step<CheckoutState, CheckoutOutcome> Decide(CheckoutState state, CheckoutFact fact) =>
        (state, fact) switch
        {
            (Reserving, Reserved r) => new Next<CheckoutState>(new Charging(r.Reservation)),
            (Charging, PaymentAnswered { Reply: Approved a }) => new Finish<CheckoutOutcome>(new OrderPlaced(a.Charge)),
            (Charging, PaymentAnswered { Reply: Refused r }) => new Finish<CheckoutOutcome>(new OrderFailed(r.Why)),

            (Reserving or Charging, WorkStuck w) => new Next<CheckoutState>(new NeedsAttention(state, w.What)),
            (NeedsAttention n, OperatorSaidRetry) => new Next<CheckoutState>(n.Was),

            (Reserving, PaymentAnswered or OperatorSaidRetry) => new Stay(),
            (Charging, Reserved or OperatorSaidRetry) => new Stay(),
            (NeedsAttention, Reserved or PaymentAnswered or WorkStuck) => new Stay(),
        };

    public static readonly EntityDefinition<WorkflowState, WorkflowCommand, WorkflowEvent> Definition =
        WorkflowWrappers.Define<CheckoutInput, CheckoutState, CheckoutFact, CheckoutOutcome>(TypeName, Start, Decide);
}

// ---------------------------------------------------------------------------------------------------
// The library's part: wrappers that are not generic. Every value of an author's type is an object field,
// which Akka.Serialization.V2 writes with whatever serializer is bound to the value's type. The author
// writes nothing for these.
// ---------------------------------------------------------------------------------------------------

public interface IWorkflowWrapperProtocol;

// State
[AkkaSerializable(Manifest = "wf-not-started-v1", AllowEmpty = true)]
public sealed record NotStarted : IWorkflowWrapperProtocol;

[AkkaSerializable(Manifest = "wf-running-v1")]
public sealed record Running(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object State,
    [property: AkkaField(2)] ImmutableArray<int> AnsweredWork,
    [property: AkkaField(3)] object? LastFact) : IWorkflowWrapperProtocol
{
    // ImmutableArray compares by reference; compare its contents so records with equal data are equal.
    public bool Equals(Running? other) =>
        other is not null && Equals(Input, other.Input) && Equals(State, other.State)
        && AnsweredWork.SequenceEqual(other.AnsweredWork) && Equals(LastFact, other.LastFact);

    public override int GetHashCode() => HashCode.Combine(Input, State, AnsweredWork.Length, LastFact);
}

[AkkaSerializable(Manifest = "wf-finished-v1")]
public sealed record Finished(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object Outcome) : IWorkflowWrapperProtocol;

[AkkaSerializable(Manifest = "wf-state-v1")]
public readonly union WorkflowState(NotStarted, Running, Finished) : IWorkflowWrapperProtocol;

// Events
[AkkaSerializable(Manifest = "wf-started-v1")]
public sealed record WorkflowStarted(
    [property: AkkaField(0)] object Input,
    [property: AkkaField(1)] object First) : IWorkflowWrapperProtocol;

[AkkaSerializable(Manifest = "wf-fact-moved-v1")]
public sealed record FactMoved(
    [property: AkkaField(0)] object Fact,
    [property: AkkaField(1)] object Next) : IWorkflowWrapperProtocol;

[AkkaSerializable(Manifest = "wf-fact-finished-v1")]
public sealed record FactFinished(
    [property: AkkaField(0)] object Fact,
    [property: AkkaField(1)] object Outcome) : IWorkflowWrapperProtocol;

[AkkaSerializable(Manifest = "wf-fact-kept-v1")]
public sealed record FactKept([property: AkkaField(0)] object Fact) : IWorkflowWrapperProtocol;

[AkkaSerializable(Manifest = "wf-event-v1")]
public readonly union WorkflowEvent(WorkflowStarted, FactMoved, FactFinished, FactKept) : IWorkflowWrapperProtocol;

// Commands
[AkkaSerializable(Manifest = "wf-start-v1")]
public sealed record StartWorkflow([property: AkkaField(0)] object Input) : IWorkflowWrapperProtocol;

[AkkaSerializable(Manifest = "wf-deliver-v1")]
public sealed record Deliver([property: AkkaField(0)] object Fact) : IWorkflowWrapperProtocol;

[AkkaSerializable(Manifest = "wf-command-v1")]
public readonly union WorkflowCommand(StartWorkflow, Deliver) : IWorkflowWrapperProtocol;

[AkkaSerializer<IWorkflowWrapperProtocol>("workflow-wrapper-probe", 22602)]
public sealed partial class WorkflowWrapperSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

// What a typed Decide returns. Never serialized.
public sealed record Next<TState>(TState State);
public sealed record Finish<TOutcome>(TOutcome Outcome);
public sealed record Stay;
public readonly union Step<TState, TOutcome>(Next<TState>, Finish<TOutcome>, Stay);

public static class WorkflowWrappers
{
    /// <summary>
    /// Builds an ordinary entity definition from an author's typed functions. The entity's state, command
    /// and event types are the wrappers; the author's values are cast back to their types on the way in.
    /// </summary>
    public static EntityDefinition<WorkflowState, WorkflowCommand, WorkflowEvent> Define<TInput, TState, TFact, TOutcome>(
        EntityTypeName typeName,
        Func<TInput, TState> start,
        Func<TState, TFact, Step<TState, TOutcome>> decide)
        where TInput : notnull where TState : notnull where TFact : notnull where TOutcome : notnull =>
        new(typeName,
            _ => new NotStarted(),
            (state, command, ctx) => (state, command) switch
            {
                (NotStarted, StartWorkflow s) => ctx.Persist(new WorkflowStarted(s.Input, start((TInput)s.Input))),
                (Running r, Deliver d) => decide((TState)r.State, (TFact)d.Fact) switch
                {
                    Next<TState> next => ctx.Persist(new FactMoved(d.Fact, next.State)),
                    Finish<TOutcome> finish => ctx.Persist(new FactFinished(d.Fact, finish.Outcome)),
                    Stay => ctx.Persist(new FactKept(d.Fact)),
                },
                _ => ctx.Ignore(),
            },
            (state, @event) => (state, @event) switch
            {
                (NotStarted, WorkflowStarted e) => new Running(e.Input, e.First, [], null),
                (Running r, FactMoved e) => r with { State = e.Next, AnsweredWork = r.AnsweredWork.Add(r.AnsweredWork.Length), LastFact = e.Fact },
                (Running r, FactKept e) => r with { LastFact = e.Fact },
                (Running r, FactFinished e) => new Finished(r.Input, e.Outcome),
                _ => state,
            });
}
