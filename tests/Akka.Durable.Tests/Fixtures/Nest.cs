using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures.Nests;

// A workflow whose state is a union with a union inside it: Outer holds Inner, and Inner holds Low or High.
// Its one piece of work never answers, so a test can read the state of a workflow that is held in its state.
// Workflows with the same input, fact and outcome but other state types: a union with one case that has no
// fields, and a generic record.

public interface INestProtocol;

[AkkaSerializable(Manifest = "nest-go-v1")]
public sealed record Go([property: AkkaField(0)] int Level) : INestProtocol;

[AkkaSerializable(Manifest = "nest-low-v1")]
public sealed record Low([property: AkkaField(0)] int Level) : INestProtocol;

[AkkaSerializable(Manifest = "nest-high-v1", AllowEmpty = true)]
public sealed record High : INestProtocol;

[AkkaSerializable(Manifest = "nest-inner-v1")]
public readonly union Inner(Low, High) : INestProtocol;

[AkkaSerializable(Manifest = "nest-settled-v1", AllowEmpty = true)]
public sealed record Settled : INestProtocol;

[AkkaSerializable(Manifest = "nest-outer-v1")]
public readonly union Outer(Inner, Settled) : INestProtocol;

[AkkaSerializable(Manifest = "nest-lone-v1")]
public readonly union Lone(Settled) : INestProtocol;

[AkkaSerializable]
public sealed record Tagged<T>([property: AkkaField(0)] T Value) : INestProtocol;

[AkkaSerializable(Manifest = "nest-released-v1", AllowEmpty = true)]
public sealed record Released : INestProtocol;

[AkkaSerializable(Manifest = "nest-fact-v1")]
public readonly union NestFact(Released) : INestProtocol;

[AkkaSerializable(Manifest = "nest-over-v1", AllowEmpty = true)]
public sealed record Over : INestProtocol;

public sealed record Hold;

[AkkaSerializer<INestProtocol>("nest-test", 22606)]
[AkkaSerializerIncludes<Tagged<int>>(Manifest = "nest-tagged-int-v1")]
public sealed partial class NestSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Nest
{
    public static readonly WorkflowDefinition<Go, Outer, NestFact, Over> Definition =
        Holding<Outer>("nests", go => new Inner(new Low(go.Level)));

    public static readonly WorkflowDefinition<Go, Lone, NestFact, Over> Alone =
        Holding<Lone>("nests-alone", _ => new Settled());

    public static readonly WorkflowDefinition<Go, Tagged<int>, NestFact, Over> Tagged =
        Holding("nests-tagged", go => new Tagged<int>(go.Level));

    /// <summary>Options under which a workflow type reads the journal of <paramref name="writer"/>, as a later deployment would.</summary>
    public static EntityOptions ReadsJournalOf(WorkflowTypeName writer) =>
        EntityOptions.Default.WithPersistenceId((id, _) =>
            EntityOptions.DefaultPersistenceIdFactory(id, new EntityTypeName(writer.Value)));

    /// <summary>A workflow that starts in the state <paramref name="start"/> gives it and is held there.</summary>
    public static WorkflowDefinition<Go, TState, NestFact, Over> Holding<TState>(
        string typeName, Func<Go, TState> start, EntityOptions? options = null) where TState : notnull =>
        new(new WorkflowTypeName(typeName), start,
            Work: (_, ctx) => ctx.Execute(new Hold()),
            Decide: (_, _, ctx) => ctx.Finish(new Over()),
            Cancel: _ => new Released(),
            GaveUp: _ => new Released(),
            Options: options);
}

public sealed class HoldHandler : IEffectHandler<Hold, NestFact>
{
    public async Task<NestFact> HandleAsync(Hold effect, CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken);
        throw new InvalidOperationException("Unreachable: the delay only ends by cancellation.");
    }
}
