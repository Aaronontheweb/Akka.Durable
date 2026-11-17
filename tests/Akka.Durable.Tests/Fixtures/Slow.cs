using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures.Slows;

// A workflow whose one piece of work never answers until the test releases it. It exists to show a stream
// waiting on a workflow that does not end in time.

public interface ISlowProtocol;

[AkkaSerializable(Manifest = "slow-input-v1")]
public sealed record SlowInput([property: AkkaField(0)] string Text) : ISlowProtocol;

[AkkaSerializable(Manifest = "slow-waiting-v1", AllowEmpty = true)]
public sealed record Waiting : ISlowProtocol;

[AkkaSerializable(Manifest = "slow-released-v1")]
public sealed record Released([property: AkkaField(0)] string Text) : ISlowProtocol;

[AkkaSerializable(Manifest = "slow-lost-v1", AllowEmpty = true)]
public sealed record SlowLost : ISlowProtocol;

[AkkaSerializable(Manifest = "slow-fact-v1")]
public readonly union SlowFact(Released, SlowLost) : ISlowProtocol;

/// <summary>What the workflow ends with.</summary>
[AkkaSerializable(Manifest = "slow-outcome-v1")]
public sealed record SlowOutcome([property: AkkaField(0)] string Text) : ISlowProtocol;

/// <summary>The work: it waits for the test's gate, so the workflow stays in flight.</summary>
public sealed record Hold(string Text);

[AkkaSerializer<ISlowProtocol>("slow-test", 22613)]
public sealed partial class SlowSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Slow
{
    public static readonly WorkflowDefinition<SlowInput, Waiting, SlowFact, SlowOutcome> Definition =
        new(new WorkflowTypeName("slows"),
            Start: _ => new Waiting(),
            Work: (_, ctx) => ctx.Execute(new Hold(ctx.Input.Text)),
            Decide: (_, fact, ctx) => fact switch
            {
                Released released => ctx.Finish(new SlowOutcome(released.Text)),
                SlowLost => ctx.Finish(new SlowOutcome("")),
            },
            Cancel: _ => new SlowLost(),
            GaveUp: _ => new SlowLost());
}

/// <summary>Waits for the gate before answering, so the workflow stays in flight until the test opens it.</summary>
public sealed class HoldHandler(Gate gate) : IEffectHandler<Hold, SlowFact>
{
    public async Task<SlowFact> HandleAsync(Hold effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken);
        return new Released(effect.Text);
    }
}
