module Akka.Durable.FSharp.Tests.CheckoutWorkflowSpecs

open System
open System.Collections.Immutable
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
    | CancelRequested of reason: string * by: string
    | OperatorSaidRetry
    | OperatorSaidAbandon

// Effects: plain records, as in the C# fixture.
type ReserveStock = { Order: string }
type ChargeCard = { Order: string; Total: int }
type UndoOrder = { Order: string }
type OpenSupportTicket = { Checkout: string; Why: string }

module Checkout =

    let start (_: PlaceOrder) : CheckoutState = Reserving

    /// The work each state needs. ctx.Execute returns a single piece of work.
    let work (state: CheckoutState) (ctx: WorkContext<PlaceOrder, CheckoutFact>) : WorkList<CheckoutFact> =
        match state with
        // F# has no implicit conversions, so one piece of work becomes the list with ToWorkList().
        | Reserving -> ctx.Execute({ ReserveStock.Order = ctx.Input.Order }).ToWorkList()
        | Charging _ -> ctx.Execute({ ChargeCard.Order = ctx.Input.Order; Total = ctx.Input.Total }).ToWorkList()
        | Undoing _ -> ctx.Execute({ UndoOrder.Order = ctx.Input.Order }).ToWorkList()
        | NeedsAttention (_, why) -> ctx.Execute({ OpenSupportTicket.Checkout = ctx.Id.Value; Why = why }).ToWorkList()

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
        | (Reserving | Charging _), CancelRequested (reason, _) -> ctx.Next(Undoing(OrderCancelled reason))
        | (Undoing _ | NeedsAttention _), CancelRequested _ -> ctx.Stay()

        // Pairs that should not happen, written out so that a new state or fact does not compile until placed.
        | Reserving, (Charged _ | Declined _ | Undone | UndoRefused | TicketOpened) -> ctx.Unexpected()
        | Charging _, (Reserved _ | OutOfStock | Undone | UndoRefused | TicketOpened) -> ctx.Unexpected()
        | Undoing _, (Reserved _ | OutOfStock | Charged _ | Declined _ | TicketOpened) -> ctx.Unexpected()
        | NeedsAttention _, (Reserved _ | OutOfStock | Charged _ | Declined _ | Undone | UndoRefused) ->
            ctx.Unexpected()
        | (Reserving | Charging _ | Undoing _), (OperatorSaidRetry | OperatorSaidAbandon) -> ctx.Unexpected()

    let cancel (request: CancelRequest) : CheckoutFact = CancelRequested(request.Reason, request.By)

    let gaveUp (failure: WorkGaveUp) : CheckoutFact = WorkStuck(failure.What, failure.Reason)

    let definition =
        WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>(
            WorkflowTypeName "checkouts", start, work, decide, cancel, gaveUp)

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
    [ for w in list.Items ->
        match w with
        | :? ExecuteWork<CheckoutFact> as execute -> execute.Effect
        | other -> failwithf "Not a call to a handler: %A" other ]

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
    match decideOf (Charging "res-1") (CancelRequested("changed my mind", "u-1")) with
    | Next (Undoing (OrderCancelled "changed my mind")) -> ()
    | other -> failwithf "unexpected %A" other

[<Fact>]
let ``Decide stays on a cancel in NeedsAttention`` () =
    match decideOf (NeedsAttention(Reserving, "w")) (CancelRequested("late", "u-1")) with
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
let ``Cancel and GaveUp map to facts`` () =
    Assert.Equal(CancelRequested("why", "u-1"), Checkout.definition.Cancel.Invoke(CancelRequest("why", "u-1")))

    let stuck = WorkGaveUp({ ReserveStock.Order = "o-1" }, GaveUpReason.Failed)
    Assert.Equal(WorkStuck("ReserveStock", GaveUpReason.Failed), Checkout.definition.GaveUp.Invoke stuck)

[<Fact>]
let ``ToWorkList makes a work list of one piece of work`` () =
    let one = workCtx.Execute({ ReserveStock.Order = "o-1" })
    let list = one.ToWorkList()
    Assert.Equal<obj list>([ ({ ReserveStock.Order = "o-1" } :> obj) ], effectsOf list)

    // It is the same list as the implicit conversion in C# gives.
    Assert.Equal(WorkList<CheckoutFact>([ one :> Work<CheckoutFact> ].ToImmutableArray()), list)

[<Fact>]
let ``A status names an F# case without fields by its union, so a caller matches on the typed state`` () =
    // An F# case that has fields is its own nested class. A case without fields is an instance of the union
    // class, so the name a status reports for it is the name of the union.
    Assert.Equal("Charging", WorkflowRunning<CheckoutState>(Charging "r", at).StateName)
    Assert.Equal("CheckoutState", WorkflowRunning<CheckoutState>(Reserving, at).StateName)

    // The state itself is the typed F# value, so the caller reads the case with a match.
    let running = WorkflowRunning<CheckoutState>(Reserving, at)
    match running.State with
    | Reserving -> ()
    | Charging _
    | Undoing _
    | NeedsAttention _ -> failwithf "Expected Reserving, got %A" running.State
