using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Durable.Tests.Fixtures.Echoes;

// The smallest workflow: one state, one piece of work, one ending. Its state and outcome are plain records,
// not unions. It exists to be hosted next to the checkout.

public interface IEchoProtocol;

[AkkaSerializable(Manifest = "echo-shout-v1")]
public sealed record Shout([property: AkkaField(0)] string Text) : IEchoProtocol;

[AkkaSerializable(Manifest = "echo-listening-v1", AllowEmpty = true)]
public sealed record Listening : IEchoProtocol;

[AkkaSerializable(Manifest = "echo-echoed-v1")]
public sealed record Echoed([property: AkkaField(0)] string Text) : IEchoProtocol;

[AkkaSerializable(Manifest = "echo-lost-v1", AllowEmpty = true)]
public sealed record EchoLost : IEchoProtocol;

[AkkaSerializable(Manifest = "echo-fact-v1")]
public readonly union EchoFact(Echoed, EchoLost) : IEchoProtocol;

[AkkaSerializable(Manifest = "echo-heard-v1")]
public sealed record Heard([property: AkkaField(0)] string Text) : IEchoProtocol;

public sealed record Repeat(string Text);

[AkkaSerializer<IEchoProtocol>("echo-test", 22602)]
public sealed partial class EchoSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

public static class Echo
{
    public static readonly WorkflowDefinition<Shout, Listening, EchoFact, Heard> Definition =
        new(new WorkflowTypeName("echoes"),
            Start: _ => new Listening(),
            Work: (_, ctx) => ctx.Execute(new Repeat(ctx.Input.Text)),
            Decide: (_, fact, ctx) => fact switch
            {
                Echoed echoed => ctx.Finish(new Heard(echoed.Text)),
                EchoLost => ctx.Finish(new Heard("")),
            },
            Kind: _ => OutcomeKind.Succeeded,
            GaveUp: _ => new EchoLost());
}

public sealed class RepeatHandler : IEffectHandler<Repeat, EchoFact>
{
    public Task<EchoFact> HandleAsync(Repeat effect, CancellationToken cancellationToken) =>
        Task.FromResult<EchoFact>(new Echoed(effect.Text));
}
