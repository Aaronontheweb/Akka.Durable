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
    let running = WorkflowRunning<Phase>(Charging "res-1", DateTimeOffset.UnixEpoch)
    Assert.Equal("Charging", running.StateName)

[<Fact>]
let ``An F# state is the F# value, and its data is read by a match with no wildcard`` () =
    let running = WorkflowRunning<Phase>(Charging "res-1", DateTimeOffset.UnixEpoch)
    match running.State with
    | Charging reservation -> Assert.Equal("res-1", reservation)
    | Idle
    | Done -> failwithf "Expected Charging, got %A" running.State

[<Theory>]
[<InlineData("idle")>]
[<InlineData("done")>]
let ``An F# case without data is the state and equals the case value`` (kind: string) =
    let expected = if kind = "idle" then Idle else Done
    let running = WorkflowRunning<Phase>(expected, DateTimeOffset.UnixEpoch)
    Assert.Equal<Phase>(expected, running.State)
