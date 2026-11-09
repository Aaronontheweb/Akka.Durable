module Akka.Durable.FSharp.Tests.TypedRequestSpecs

open System
open System.Threading.Tasks
open Akka.Durable
open Akka.Durable.TestKit
open Xunit

type Reply =
    | Accepted of int
    | Rejected of string

type Event = Added of int

type Command =
    | Issue of Request
    | Computed of Request * int
    | Failed of Request * string
and Request =
    { Amount: int }
    interface IReplies<Command, Reply> with
        member this.ToCommand() = Issue this

/// The effect: plain data the decider hands to a registered handler.
type Compute = { Request: Request; Value: int }

// This signature only compiles if the command supplies the Ask reply type.
let inferAsk (entity: IEntityRef<Command>) (command: Request) : Task<Reply> =
    entity.Ask(command)

let definition =
    EntityDefinition<int, Command, Event>(
        EntityTypeName("fsharp"),
        (fun _ -> 0),
        (fun _ command ctx ->
            match command with
            | Issue c -> ctx.None().Execute({ Request = c; Value = c.Amount }).ToDecision()
            | Computed (c, result) -> ctx.Persist(Added result).Reply(c, Accepted result).ToDecision()
            | Failed (c, reason) -> ctx.None().Reply(c, Rejected reason).ToDecision()),
        (fun state (Added value) -> state + value))

[<Fact>]
let ``record command converts to its native FSharp DU without reflection`` () =
    let command = { Amount = 3 }
    let contract = command :> IReplies<Command, Reply>
    Assert.Equal(Issue command, contract.ToCommand())

[<Fact>]
let ``request issues its effect as data`` () =
    let command = { Amount = 3 }
    DeciderScenario.For(definition)
        .When(Issue command)
        .ThenNothingPersisted()
        .ThenExecuted({ Request = command; Value = 3 })
    |> ignore

[<Fact>]
let ``success outcome persists and replies with the declared FSharp DU`` () =
    let command = { Amount = 3 }
    DeciderScenario.For(definition)
        .When(Computed(command, 7))
        .ThenPersisted(Added 7)
        .ThenReplied(command, Accepted 7)
        .ThenState(fun state -> Assert.Equal(7, state))
    |> ignore

[<Fact>]
let ``failure outcome replies without persisting`` () =
    let command = { Amount = 3 }
    DeciderScenario.For(definition)
        .When(Failed(command, "failed"))
        .ThenNothingPersisted()
        .ThenReplied(command, Rejected "failed")
    |> ignore
