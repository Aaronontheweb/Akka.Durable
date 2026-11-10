module Akka.Durable.FSharp.Tests.WorkflowStateNameSpecs

open System
open Akka.Durable
open Xunit

type Phase =
    | Idle
    | Charging of reservation: string
    | Done

[<Fact>]
let ``An F# case with data reports the case name and keeps the case object`` () =
    let running = WorkflowRunning(Charging "res-1", DateTimeOffset.UnixEpoch)
    Assert.Equal("Charging", running.StateName)
    Assert.Equal(box (Charging "res-1"), running.State)

[<Theory>]
[<InlineData("idle", "Idle")>]
[<InlineData("done", "Done")>]
let ``An F# case without data reports the case name`` (kind: string, expected: string) =
    let state = if kind = "idle" then Idle else Done
    Assert.Equal(expected, WorkflowRunning(state, DateTimeOffset.UnixEpoch).StateName)
