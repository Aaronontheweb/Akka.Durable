using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Echoes;
using Akka.Durable.Tests.Fixtures.Slows;
using Akka.Durable.TestKit;
using Akka.Hosting;
using Akka.Remote.Hosting;
using Akka.Streams;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Hosts the echo workflow and the counter entity through the public API, the way an application would, and
/// gives the stream specs a materializer. Subclasses choose the hosting mode.
/// </summary>
public abstract class StreamKit(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    /// <summary>Host in a shard region of a single-node cluster; otherwise in a local host actor.</summary>
    protected virtual bool Sharded => false;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
        services.AddCounterHandlers();
        services.AddEffectHandler<Repeat, EchoFact, RepeatHandler>();
        services.AddEffectHandler<Hold, SlowFact, HoldHandler>();
    }

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        if (Sharded)
            builder.WithRemoting("localhost", 0).WithClustering().AddStartup((system, _) =>
            {
                var cluster = Akka.Cluster.Cluster.Get(system);
                cluster.Join(cluster.SelfAddress);
            });

        builder
            .WithCustomSerializer(EchoSerializer.CreateSerializerDetails)
            .WithCustomSerializer(SlowSerializer.CreateSerializerDetails)
            .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
            .WithDurableWorkflow(Echo.Definition, Sharding())
            .WithDurableWorkflow(Slow.Definition, Sharding())
            .WithDurableEntity(Counter.Definition, Sharding());
    }

    protected ShardedHosting? Sharding() => Sharded ? new ShardedHosting() : null;

    protected IWorkflowRef<Shout, Listening, EchoFact, Heard> EchoRef(string id) =>
        WorkflowRef<Shout, Listening, EchoFact, Heard>(id);

    protected IWorkflowRefs<Shout, Listening, EchoFact, Heard> Echoes() =>
        Host.Services.GetRequiredService<IWorkflowRefs<Shout, Listening, EchoFact, Heard>>();

    protected IWorkflowRefs<SlowInput, Waiting, SlowFact, SlowOutcome> Slows() =>
        Host.Services.GetRequiredService<IWorkflowRefs<SlowInput, Waiting, SlowFact, SlowOutcome>>();

    protected IDurableEntityRef<CounterCommand, CounterState, CounterEvent> CounterRef(string id) =>
        DurableEntityRef<CounterCommand, CounterState, CounterEvent>(id);

    /// <summary>The materializer the test host's actor system uses.</summary>
    protected IMaterializer Materializer => Sys.Materializer();
}
