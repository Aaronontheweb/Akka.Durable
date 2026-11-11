using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// An entity asked to stop while an <c>Execute</c> handler is in flight stops later. These are the two ways
/// that wait can be disturbed: the entity restarts, or the handler's task faults before it reports.
/// </summary>
public abstract class DeferredStopSpec(ITestOutputHelper output) : CounterHostingSpec(output)
{
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(1);

    // The first message waits for the cluster and shard coordinator to come up in sharded mode.
    private static readonly TimeSpan FirstAskTimeout = TimeSpan.FromSeconds(30);

    protected override EntityDefinition<CounterState, CounterCommand, CounterEvent> Definition =>
        Counter.Definition with { Options = EntityOptions.Default with { PassivateIdleAfter = IdleTimeout } };

    protected abstract string EntityPath(string id);

    private Task<IActorRef> EntityActorOf(string id) =>
        Sys.ActorSelection(EntityPath(id)).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);

    /// <summary>
    /// Starts a service increment whose handler waits for the gate, waits until the host has asked the entity
    /// to stop, and sends a message that the host buffers until the entity is gone.
    /// </summary>
    protected async Task<IActorRef> StopDeferredWithHandlerInFlight(IEntityRef<CounterCommand> counter)
    {
        await counter.Ask(new GetCount(), AskCancellation(FirstAskTimeout));
        var entity = await EntityActorOf("a");
        Watch(entity);

        counter.Tell(new IncrementViaService(5, Wait: true), TestActor);

        // Several idle timeouts: the host asks the entity to stop, and the entity stays up.
        await ExpectNoMsgAsync(IdleTimeout * 3, TestCancellation);

        counter.Tell(new GetCount(), TestActor);
        return entity;
    }
}

/// <summary>
/// A restart while the stop is deferred. The host asks for the stop once and then buffers. If the restarted
/// entity forgot the request, nothing would ask again and it would live on with its messages held back.
/// </summary>
public abstract class RestartWhileStopDeferredSpec(ITestOutputHelper output) : DeferredStopSpec(output)
{
    [Fact]
    public async Task A_restart_while_a_stop_is_deferred_still_ends_in_a_stop()
    {
        var counter = CounterRef("a");
        var entity = await StopDeferredWithHandlerInFlight(counter);

        // A command with no case in Decide: the decision throws and the entity restarts.
        entity.Tell(default(CounterCommand));

        // The restarted entity recovers the effect that is still pending. It is not run again: it gives up as
        // interrupted, and nothing is in flight any more, so the stop the host asked for before the restart happens.
        // The give-up's reply goes to dead letters: the asker belongs to the first incarnation.
        var received = await ReceiveNAsync(2, cancellationToken: TestCancellation).ToListAsync(TestCancellation);
        Assert.Contains(received.OfType<Terminated>(), t => t.ActorRef.Equals(entity));

        // The buffered message was answered by a new incarnation. The increment gave up, so the count did not move.
        Assert.Equal(0, Assert.Single(received.OfType<CountIs>()).Count);
        Assert.Equal(1, Recorder.Entries.Count(e => e == "compute:5"));
        Assert.NotEqual(entity, await Sys.ActorSelection(EntityPath("a")).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation));
    }
}

public class LocalRestartWhileStopDeferredSpecs(ITestOutputHelper output) : RestartWhileStopDeferredSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    protected override string EntityPath(string id) =>
        $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/{Uri.EscapeDataString(id)}";
}

public class ShardedRestartWhileStopDeferredSpecs(ITestOutputHelper output) : RestartWhileStopDeferredSpec(output)
{
    protected override ShardedHosting? Sharding => new();

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder) => ConfigureSingleNodeCluster(builder);

    protected override string EntityPath(string id) => $"/system/sharding/{Counter.TypeName.Value}/*/{id}";
}

/// <summary>
/// A handler whose task faults before it reports. The handler's own failures are caught, so the one way to
/// fault is the scope's disposal throwing after the handler has finished.
/// </summary>
public abstract class FaultingHandlerStopDeferredSpec(ITestOutputHelper output) : DeferredStopSpec(output)
{
    // The counter's services, with this handler for Compute instead of the counter's own.
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddEffectHandler<Compute, CounterCommand, ComputeHandlerThatFaultsOnDisposal>();

    [Fact]
    public async Task A_handler_whose_task_faults_before_it_reports_still_lets_the_entity_stop()
    {
        var counter = CounterRef("a");
        var entity = await StopDeferredWithHandlerInFlight(counter);

        // The handler finishes, its scope fails to dispose, and its answer is lost: the effect gives up as failed.
        Gate.Open();

        // The give-up answers the asker, the entity stops, and the buffered message is answered by a new incarnation.
        // The increment gave up, so the count has not moved.
        var received = await ReceiveNAsync(3, cancellationToken: TestCancellation).ToListAsync(TestCancellation);
        Assert.Single(received.OfType<Accepted>());
        Assert.Contains(received.OfType<Terminated>(), t => t.ActorRef.Equals(entity));
        Assert.Equal(0, Assert.Single(received.OfType<CountIs>()).Count);
    }
}

public class LocalFaultingHandlerStopDeferredSpecs(ITestOutputHelper output) : FaultingHandlerStopDeferredSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    protected override string EntityPath(string id) =>
        $"/user/{LocalEntityHost.ActorName(Counter.TypeName)}/{Uri.EscapeDataString(id)}";
}

public class ShardedFaultingHandlerStopDeferredSpecs(ITestOutputHelper output) : FaultingHandlerStopDeferredSpec(output)
{
    protected override ShardedHosting? Sharding => new();

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder) => ConfigureSingleNodeCluster(builder);

    protected override string EntityPath(string id) => $"/system/sharding/{Counter.TypeName.Value}/*/{id}";
}

/// <summary>Waits for the gate like the counter's own handler, then fails when its DI scope is disposed.</summary>
public sealed class ComputeHandlerThatFaultsOnDisposal(Gate gate) : IEffectHandler<Compute, CounterCommand>, IAsyncDisposable
{
    public async Task<CounterCommand> HandleAsync(Compute effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        if (effect.Request.Wait) await gate.WaitAsync(cancellationToken);
        return new Computed(effect.Request, effect.Request.By * 2);
    }

    public ValueTask DisposeAsync() => throw new InvalidOperationException("The scope cannot be disposed.");
}
