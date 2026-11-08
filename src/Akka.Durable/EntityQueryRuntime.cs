using Akka.Actor;
using Akka.Streams;

namespace Akka.Durable;

/// <summary>Owns one context-scoped query materializer per actor system.</summary>
internal sealed class EntityQueryRuntime : IExtension
{
    private EntityQueryRuntime(ExtendedActorSystem system)
    {
        var ready = new TaskCompletionSource<ActorMaterializer>(TaskCreationOptions.RunContinuationsAsynchronously);
        Materializer = ready.Task;
        Owner = system.SystemActorOf(Props.Create(() => new QueryOwner(ready)), "durable-queries");
    }

    public IActorRef Owner { get; }
    public Task<ActorMaterializer> Materializer { get; }

    public static EntityQueryRuntime Get(ActorSystem system) => Provider.Instance.Apply(system);

    private sealed class Provider : ExtensionIdProvider<EntityQueryRuntime>
    {
        public static readonly Provider Instance = new();
        public override EntityQueryRuntime CreateExtension(ExtendedActorSystem system) => new(system);
    }

    private sealed class QueryOwner : UntypedActor
    {
        private readonly ActorMaterializer? _materializer;

        public QueryOwner(TaskCompletionSource<ActorMaterializer> ready)
        {
            try
            {
                // Passing Context creates an owned supervisor beneath this system actor.
                _materializer = ActorMaterializer.Create(Context, namePrefix: "durable-query");
                ready.TrySetResult(_materializer);
            }
            catch (Exception error)
            {
                ready.TrySetException(error);
                throw;
            }
        }

        protected override void OnReceive(object message) => Unhandled(message);

        protected override void PostStop()
        {
            _materializer?.Shutdown();
            base.PostStop();
        }
    }
}
