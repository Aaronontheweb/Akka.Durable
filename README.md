# Akka.Durable

Akka.Durable is an early-stage project exploring durable entities and workflows on Akka.NET. The intended foundation is Akka.Persistence and Cluster Sharding, with a local development and execution story as well as clustered deployments. Agent capabilities are a much later possibility, not part of the initial scope.

The design draws on a functional event-sourcing decider: pure decisions and state transitions, with persistence, recovery, delivery, and other effects handled by a runtime shell. The design is exploratory; none of these capabilities is implemented yet.

## Getting started

Install the .NET 11 RC SDK pinned in `global.json` (including prerelease SDKs), then run:

```sh
dotnet restore
dotnet build
dotnet test
```

The solution targets `net11.0` and enables preview C# features. Package versions are managed centrally; Akka.Cluster.Hosting is pinned to 1.6.0-beta5. The existing console application and tests are template samples, not the durable runtime. Run the sample with `dotnet run --project src/Akka.Console`.

## Direction

- Durable entities backed by Akka.Persistence.
- Durable workflows coordinating work across entities, using Cluster Sharding for distributed placement.
- A local story for development and execution.
- Agents much later, once the foundational capabilities exist.

The API, persistence guarantees, local deployment model, and release plan remain to be designed.
