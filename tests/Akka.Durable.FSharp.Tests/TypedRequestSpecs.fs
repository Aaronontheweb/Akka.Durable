module Akka.Durable.FSharp.Tests.TypedRequestSpecs

open System
open System.Threading
open System.Threading.Tasks
open Akka.Durable
open Akka.Durable.TestKit
open Xunit

type Reply =
    | Accepted of int
    | Rejected of string

type Event = Added of int

type Command = Issue of Request
and Request =
    { Amount: int }
    interface IReplies<Command, Reply> with
        member this.ToCommand() = Issue this

type Service() =
    member _.ExecuteAsync(value: int, _: CancellationToken) = Task.FromResult(value)

// This signature only compiles if the command supplies the Ask reply type.
let inferAsk (entity: IEntityRef<Command>) (command: Request) : Task<Reply> =
    entity.Ask(command)

let definition =
    EntityDefinition<int, Command, Event>(
        EntityTypeName("fsharp"),
        (fun _ -> 0),
        (fun _ command ctx ->
            match command with
            | Issue c ->
                let request = ctx.For(c)
                request.Run<Service, int>(
                    (fun (service: Service) ct -> service.ExecuteAsync(c.Amount, ct)),
                    (fun result next -> next.Persist(Added result).Reply(Accepted result)),
                    (fun error next -> next.Reply(Rejected error.Message)))
                    .ToDecision()),
        (fun state (Added value) -> state + value))

[<Fact>]
let ``record command converts to its native FSharp DU without reflection`` () =
    let command = { Amount = 3 }
    let contract = command :> IReplies<Command, Reply>
    Assert.Equal(Issue command, contract.ToCommand())

[<Fact>]
let ``result continuation persists and replies with the declared FSharp DU`` () =
    let command = { Amount = 3 }
    DeciderScenario.For(definition)
        .When(Issue command)
        .WhenRunSucceeded(7)
        .ThenPersisted(Added 7)
        .ThenReplied(command, Accepted 7)
        .ThenState(fun state -> Assert.Equal(7, state))
    |> ignore

[<Fact>]
let ``failure continuation replies without persisting success`` () =
    let command = { Amount = 3 }
    DeciderScenario.For(definition)
        .When(Issue command)
        .WhenRunFailed(InvalidOperationException("failed"))
        .ThenNothingPersisted()
        .ThenReplied(command, Rejected "failed")
    |> ignore

[<Fact>]
let ``early replies allow non reply continuations using native FSharp values`` () =
    let earlyDefinition =
        EntityDefinition<int, Command, Event>(
            EntityTypeName("fsharp-early"),
            (fun _ -> 0),
            (fun _ command ctx ->
                match command with
                | Issue c ->
                    ctx.For(c).Persist(Added 1).ReplyAndRun<Service, int>(
                        Accepted 1,
                        (fun (service: Service) ct -> service.ExecuteAsync(c.Amount, ct)),
                        (fun result next -> next.Persist(Added result).ToEffectDecision()),
                        (fun _ next -> next.ToEffectDecision()))
                        .ToDecision()),
            (fun state (Added value) -> state + value))
    let command = { Amount = 3 }
    let outcome =
        DeciderScenario.For(earlyDefinition)
            .When(Issue command)
            .ThenPersisted(Added 1)
            .ThenReplied(command, Accepted 1)
    outcome.WhenRunSucceeded(7)
        .ThenPersisted(Added 7)
        .ThenEffects()
        .ThenState(fun state -> Assert.Equal(8, state))
    |> ignore
    outcome.WhenRunFailed(InvalidOperationException("failed"))
        .ThenNothingPersisted()
        .ThenEffects()
        .ThenState(fun state -> Assert.Equal(1, state))
    |> ignore
