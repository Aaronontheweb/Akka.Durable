module Akka.Durable.FSharp.Tests.CheckoutWorkflowSpecs

// F# reports every use of an implicit conversion (FS3391), and this project treats warnings as errors.
// Work<_> converts to WorkList<_> only by the C# implicit operator, so this file opts out of that warning.
#nowarn "3391"

open System
open Akka.Durable
open Xunit

// The checkout workflow of the C# fixture (Akka.Durable.Tests/Fixtures/Checkout.cs), written in F#.
// F# unions have no generated Akka serializer, so these tests call the definition's functions directly
// and never host the workflow.

// Input
type PlaceOrder = { Order: string; Total: int }

// Outcomes
type CheckoutOutcome =
    | OrderPlaced of charge: string
    | OrderFailed of reason: string
    | OrderCancelled of reason: string

// States. NeedsAttention holds the state it came from: the state union inside one of its own cases.
type CheckoutState =
    | Reserving
    | Charging of reservation: string
    | Undoing of ``then``: CheckoutOutcome
    | NeedsAttention of was: CheckoutState * why: string

// Facts
type CheckoutFact =
    | Reserved of reservation: string
    | OutOfStock
    | Charged of charge: string
    | Declined of reason: string
    | Undone
    | UndoRefused
    | WorkStuck of what: string * reason: GaveUpReason
    | TicketOpened
    | CancelRequested of reason: string
    | OperatorSaidRetry
    | OperatorSaidAbandon

// Effects: plain records, as in the C# fixture.
type ReserveStock = { Order: string }
type ChargeCard = { Order: string; Total: int }
type UndoOrder = { Order: string }
type OpenSupportTicket = { Checkout: string; Why: string }

module Checkout =

    let start (_: PlaceOrder) : CheckoutState = Reserving

    /// The work each state needs. ctx.Execute returns a single piece of work; the WorkList<_> is built by
    /// the implicit conversion, which F# applies because the declared return type is known.
    let work (state: CheckoutState) (ctx: WorkContext<PlaceOrder, CheckoutFact>) : WorkList<CheckoutFact> =
        match state with
        | Reserving -> ctx.Execute({ ReserveStock.Order = ctx.Input.Order })
        | Charging _ -> ctx.Execute({ ChargeCard.Order = ctx.Input.Order; Total = ctx.Input.Total })
        | Undoing _ -> ctx.Execute({ UndoOrder.Order = ctx.Input.Order })
        | NeedsAttention (_, why) -> ctx.Execute({ OpenSupportTicket.Checkout = ctx.Id.Value; Why = why })

    /// The fact table. No wildcard arm: a new state or fact is an FS0025 warning, and this project
    /// treats warnings as errors, so it does not compile until the pair is placed.
    let decide
        (state: CheckoutState)
        (fact: CheckoutFact)
        (ctx: DecideContext<PlaceOrder, CheckoutState, CheckoutOutcome>)
        : Transition<CheckoutState, CheckoutOutcome> =
        match state, fact with
        | Reserving, Reserved reservation -> ctx.Next(Charging reservation)
        | Reserving, OutOfStock -> ctx.Finish(OrderFailed "out of stock")

        | Charging _, Charged charge -> ctx.Finish(OrderPlaced charge)
        | Charging _, Declined reason -> ctx.Next(Undoing(OrderFailed reason))

        | Undoing next, Undone -> ctx.Finish next
        | Undoing _, UndoRefused -> ctx.Again()

        // Work that got no answer: a person decides. The state it came from is kept.
        | (Reserving | Charging _ | Undoing _), WorkStuck (what, reason) ->
            ctx.Next(NeedsAttention(state, $"{what} {reason}"))
        | NeedsAttention _, TicketOpened -> ctx.Stay()
        | NeedsAttention _, WorkStuck _ -> ctx.Stay()

        // A person decides what happens to a checkout that needs attention.
        | NeedsAttention (was, _), OperatorSaidRetry -> ctx.Next was
        | NeedsAttention (_, why), OperatorSaidAbandon -> ctx.Next(Undoing(OrderFailed why))

        // A cancel undoes the order, unless it is already being undone or waits for a person.
        | (Reserving | Charging _), CancelRequested reason -> ctx.Next(Undoing(OrderCancelled reason))
        | (Undoing _ | NeedsAttention _), CancelRequested _ -> ctx.Stay()

        // Pairs that should not happen, written out so that a new state or fact does not compile until placed.
        | Reserving, (Charged _ | Declined _ | Undone | UndoRefused | TicketOpened) -> ctx.Unexpected()
        | Charging _, (Reserved _ | OutOfStock | Undone | UndoRefused | TicketOpened) -> ctx.Unexpected()
        | Undoing _, (Reserved _ | OutOfStock | Charged _ | Declined _ | TicketOpened) -> ctx.Unexpected()
        | NeedsAttention _, (Reserved _ | OutOfStock | Charged _ | Declined _ | Undone | UndoRefused) ->
            ctx.Unexpected()
        | (Reserving | Charging _ | Undoing _), (OperatorSaidRetry | OperatorSaidAbandon) -> ctx.Unexpected()

    let kind (outcome: CheckoutOutcome) : OutcomeKind =
        match outcome with
        | OrderPlaced _ -> OutcomeKind.Succeeded
        | OrderFailed _ -> OutcomeKind.Failed
        | OrderCancelled _ -> OutcomeKind.Cancelled

    let cancel (request: CancelRequest) : CheckoutFact = CancelRequested request.Reason

    let gaveUp (failure: WorkGaveUp) : CheckoutFact = WorkStuck(failure.What, failure.Reason)

    let definition =
        WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>(
            WorkflowTypeName "checkouts", start, work, decide, kind, cancel, gaveUp)

// Reading the C# union Transition<_,_> from F#: its Value is the case record. This turns it into an
// F# active pattern so tests can match on it.
let (|Next|Finish|Stay|Again|Unexpected|) (transition: Transition<CheckoutState, CheckoutOutcome>) =
    match transition.Value with
    | :? NextTransition<CheckoutState> as next -> Next next.State
    | :? FinishTransition<CheckoutOutcome> as finish -> Finish finish.Outcome
    | :? StayTransition -> Stay
    | :? AgainTransition -> Again
    | :? UnexpectedTransition -> Unexpected
    | other -> failwithf "Not a transition case: %A" other

let private input = { PlaceOrder.Order = "o-1"; Total = 40 }
let private workflowId = WorkflowId "checkout-1"
let private at = DateTimeOffset.UnixEpoch

let private workCtx = WorkContext<PlaceOrder, CheckoutFact>(workflowId, input, at)

let private decideCtx =
    DecideContext<PlaceOrder, CheckoutState, CheckoutOutcome>(workflowId, input, at, at)

let private effectsOf (list: WorkList<CheckoutFact>) : obj list =
    [ for w in list.Items -> w.Effect ]

let private workOf state = Checkout.definition.Work.Invoke(state, workCtx)
let private decideOf state fact = Checkout.definition.Decide.Invoke(state, fact, decideCtx)

[<Fact>]
let ``Start gives the first state`` () =
    Assert.Equal(Reserving, Checkout.definition.Start.Invoke input)

[<Fact>]
let ``Work gives the effect each state needs`` () =
    let cases : (CheckoutState * obj) list =
        [ Reserving, ({ ReserveStock.Order = "o-1" } :> obj)
          Charging "res-1", ({ ChargeCard.Order = "o-1"; Total = 40 } :> obj)
          Undoing(OrderFailed "x"), ({ UndoOrder.Order = "o-1" } :> obj)
          NeedsAttention(Reserving, "ReserveStock TimedOut"),
          ({ OpenSupportTicket.Checkout = "checkout-1"; Why = "ReserveStock TimedOut" } :> obj) ]

    for state, effect in cases do
        Assert.Equal<obj list>([ effect ], effectsOf (workOf state))

[<Fact>]
let ``Work gives an equal result when called twice`` () =
    for state in [ Reserving; Charging "res-1"; Undoing(OrderCancelled "c"); NeedsAttention(Reserving, "w") ] do
        Assert.Equal(workOf state, workOf state)

[<Fact>]
let ``Decide on the happy path reserves, charges, and finishes`` () =
    match decideOf Reserving (Reserved "res-1") with
    | Next (Charging "res-1") -> ()
    | other -> failwithf "unexpected %A" other

    match decideOf (Charging "res-1") (Charged "ch-1") with
    | Finish (OrderPlaced "ch-1") -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Decide undoes the order when the charge is declined`` () =
    match decideOf (Charging "res-1") (Declined "no funds") with
    | Next (Undoing (OrderFailed "no funds")) -> ()
    | other -> failwithf "unexpected %A" other

    match decideOf (Undoing(OrderFailed "no funds")) Undone with
    | Finish (OrderFailed "no funds") -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Decide sends a working state that got no answer to NeedsAttention`` () =
    match decideOf (Charging "res-1") (WorkStuck("ChargeCard", GaveUpReason.TimedOut)) with
    | Next (NeedsAttention (Charging "res-1", "ChargeCard TimedOut")) -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Decide undoes the order on a cancel in a working state`` () =
    match decideOf (Charging "res-1") (CancelRequested "changed my mind") with
    | Next (Undoing (OrderCancelled "changed my mind")) -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Decide stays on a cancel in NeedsAttention`` () =
    match decideOf (NeedsAttention(Reserving, "w")) (CancelRequested "late") with
    | Stay -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Decide retries the state an operator sends back`` () =
    match decideOf (NeedsAttention(Charging "res-1", "w")) OperatorSaidRetry with
    | Next (Charging "res-1") -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Decide issues an undo again when it is refused`` () =
    match decideOf (Undoing(OrderFailed "x")) UndoRefused with
    | Again -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Decide flags a pair that should not happen`` () =
    match decideOf Reserving (Charged "ch-1") with
    | Unexpected -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Kind classifies each outcome`` () =
    let kind = Checkout.definition.Kind
    Assert.Equal(OutcomeKind.Succeeded, kind.Invoke(OrderPlaced "ch-1"))
    Assert.Equal(OutcomeKind.Failed, kind.Invoke(OrderFailed "x"))
    Assert.Equal(OutcomeKind.Cancelled, kind.Invoke(OrderCancelled "x"))

[<Fact>]
let ``Cancel and GaveUp map to facts`` () =
    Assert.Equal(CancelRequested "why", Checkout.definition.Cancel.Invoke(CancelRequest("why")))

    let stuck = WorkGaveUp({ ReserveStock.Order = "o-1" }, GaveUpReason.Failed)
    Assert.Equal(WorkStuck("ReserveStock", GaveUpReason.Failed), Checkout.definition.GaveUp.Invoke stuck)

[<Fact>]
let ``The status read names a state with fields by its case but a state without fields by its union`` () =
    // WorkflowRef names a state with IUnion, else GetType().Name. An F# case with fields is its own nested
    // class; a case without fields is an instance of the union class. This pins the current behavior: a
    // status read of a Reserving workflow says "CheckoutState", not "Reserving".
    let name (state: CheckoutState) = state.GetType().Name
    Assert.Equal("Charging", name (Charging "r"))
    Assert.Equal("CheckoutState", name Reserving)
