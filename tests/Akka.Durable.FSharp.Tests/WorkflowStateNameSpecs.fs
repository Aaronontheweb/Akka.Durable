module Akka.Durable.FSharp.Tests.WorkflowStateNameSpecs

open System
open Akka.Durable
open Xunit

type Phase =
    | Idle
    | Charging of reservation: string
    | Done

// The display name of an F# case without data is not covered here.

[<Fact>]
let ``An F# case with data reports the case name`` () =
    let running = WorkflowRunning(Charging "res-1", DateTimeOffset.UnixEpoch)
    Assert.Equal("Charging", running.StateName)

[<Fact>]
let ``An F# case with data is the state, and its data can be read back`` () =
    let running = WorkflowRunning(Charging "res-1", DateTimeOffset.UnixEpoch)
    match running.State with
    | Charging reservation -> Assert.Equal("res-1", reservation)
    | other -> failwithf "Expected Charging, got %A" other

[<Theory>]
[<InlineData("idle")>]
[<InlineData("done")>]
let ``An F# case without data is the state and equals the case value`` (kind: string) =
    let expected = if kind = "idle" then Idle else Done
    let running = WorkflowRunning(expected, DateTimeOffset.UnixEpoch)
    Assert.Equal(box expected, running.State)
