using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Hosting;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Hosts the checkout workflow through the public API, the way an application would, and gives specs a
/// view of the entity that hosts it. Subclasses choose the hosting mode, the definition and the handlers.
/// </summary>
public abstract class CheckoutWorkflowKit(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(20);

    /// <summary>Host in a shard region of a single-node cluster; otherwise in a local host actor.</summary>
    protected virtual bool Sharded => false;

    protected virtual WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        Checkout.Definition;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddCheckoutHandlers();

    /// <summary>Other entity types, workflow types and plugins for the test host.</summary>
    protected virtual void ConfigureMore(AkkaConfigurationBuilder builder)
    {
    }

    protected sealed override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        if (Sharded)
            builder.WithRemoting("localhost", 0).WithClustering().AddStartup((system, _) =>
            {
                var cluster = Akka.Cluster.Cluster.Get(system);
                cluster.Join(cluster.SelfAddress);
            });

        builder
            .WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails)
            .WithDurableWorkflow(Definition, Sharding());

        ConfigureMore(builder);
    }

    protected ShardedHosting? Sharding() => Sharded ? new ShardedHosting() : null;

    protected CheckoutServices Services => Host.Services.GetRequiredService<CheckoutServices>();

    protected IWorkflowRef<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> CheckoutRef(string id) =>
        WorkflowRef<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>(id);

    /// <summary>The state of the entity that hosts a checkout: the author's state and what the library tracks.</summary>
    private protected async Task<StateSnapshot<WfState>> HostedState(string id)
    {
        var region = Host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>>().ActorRef;
        return await new EntityRef<WfCommand, WfState, WfEvent>(new EntityId(id), region, Sys).GetStateAsync(AskCancellation());
    }

    private protected async Task<WfRunning> AwaitRunning(string id, Action<WfRunning> assert)
    {
        WfRunning? found = null;
        await AwaitAssertAsync(async () =>
        {
            found = Assert.IsType<WfRunning>((await HostedState(id)).State.Value);
            assert(found);
        }, cancellationToken: TestCancellation);
        return found!;
    }

    /// <summary>Waits until the workflow is in <typeparamref name="TState"/> and that state's work has been answered.</summary>
    private protected Task<WfRunning> SettledIn<TState>(string id) => AwaitRunning(id, running =>
    {
        Assert.IsType<TState>(((CheckoutState)running.State).Value);
        Assert.Equal(new[] { 0 }, running.Settled);
    });

    /// <summary>
    /// Kills the workflow's actor and waits until its host has started a new one, which has to recover.
    /// </summary>
    /// <remarks>
    /// The host learns that the actor stopped a moment after this spec does. A message sent in between goes to
    /// the dead actor and is lost. So this reads the state, with a short time limit, until the new actor answers.
    /// </remarks>
    protected async Task RestartWorkflow(string id)
    {
        await StopWorkflow(id);

        await AwaitAssertAsync(async () =>
        {
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(TestCancellation);
            attempt.CancelAfter(TimeSpan.FromMilliseconds(500));
            var region = Host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome>>>().ActorRef;
            await new EntityRef<WfCommand, WfState, WfEvent>(new EntityId(id), region, Sys).GetStateAsync(attempt.Token);
        }, TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
    }

    /// <summary>
    /// Kills the workflow's actor and sends it nothing. Whatever reaches the workflow next starts a new actor,
    /// which has to recover. A message sent straight after this can be lost: see <see cref="RestartWorkflow"/>.
    /// </summary>
    protected async Task StopWorkflow(string id)
    {
        var type = Definition.TypeName.Value;
        var path = Sharded
            ? $"/system/sharding/{type}/*/{id}"
            : $"/user/{LocalEntityHost.ActorName(new EntityTypeName(type))}/{id}";
        var actor = await Sys.ActorSelection(path).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);
        Watch(actor);
        actor.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(actor, cancellationToken: TestCancellation);
    }

    /// <summary>A service call that gets no answer for as long as the workflow's actor lives.</summary>
    protected static async Task<CheckoutFact> Never(CancellationToken ct)
    {
        await Task.Delay(Timeout.Infinite, ct);
        throw new InvalidOperationException("Unreachable: the delay only ends by cancellation.");
    }
}
