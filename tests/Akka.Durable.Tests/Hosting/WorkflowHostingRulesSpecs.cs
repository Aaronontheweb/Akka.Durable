using Akka.Cluster.Hosting;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Durable.Tests.Fixtures.Echoes;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// What <c>WithDurableWorkflow</c> refuses: a second workflow type for the same input and outcome types,
/// and a workflow whose own types have no serializer.
/// </summary>
public class WorkflowHostingRulesSpecs
{
    private static readonly WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> SecondCheckout =
        Checkout.Definition with { TypeName = new WorkflowTypeName("checkouts-two") };

    private static AkkaConfigurationBuilder Builder() => new(new ServiceCollection(), "test");

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Two_workflow_types_cannot_share_their_input_and_outcome_types(bool sharded)
    {
        var builder = Builder().WithDurableWorkflow(Checkout.Definition, sharded ? new ShardedHosting() : null);

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.WithDurableWorkflow(SecondCheckout, sharded ? new ShardedHosting() : null));

        Assert.Contains("[checkouts]", ex.Message);
        Assert.Contains("[checkouts-two]", ex.Message);
        Assert.Contains(typeof(PlaceOrder).FullName!, ex.Message);
    }

    [Fact]
    public void A_workflow_type_cannot_be_registered_twice()
    {
        var builder = Builder().WithDurableWorkflow(Checkout.Definition);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.WithDurableWorkflow(Checkout.Definition));

        Assert.Contains("[checkouts] is registered twice", ex.Message);
    }

    [Fact]
    public void Workflow_types_with_their_own_input_types_register_together()
    {
        var builder = Builder();

        Assert.Same(builder, builder.WithDurableWorkflow(Checkout.Definition).WithDurableWorkflow(Echo.Definition));
    }

    // Deliberately carry no [AkkaSerializable] and no serializer registration.
    public sealed record PlainInput(int Value);
    public sealed record PlainState;
    public sealed record PlainFact;
    public sealed record PlainOutcome;
    public sealed record PlainWork;

    private static readonly WorkflowDefinition<PlainInput, PlainState, PlainFact, PlainOutcome> PlainDefinition = new(
        new WorkflowTypeName("plain"),
        Start: _ => new PlainState(),
        Work: (_, ctx) => ctx.Execute(new PlainWork()),
        Decide: (_, _, ctx) => ctx.Finish(new PlainOutcome()),
        Kind: _ => OutcomeKind.Succeeded,
        GaveUp: _ => new PlainFact());

    [Fact]
    public async Task Workflow_types_without_a_serializer_fail_host_startup()
    {
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddAkka("workflow-serializer-requirement", builder => builder.WithDurableWorkflow(PlainDefinition));
            })
            .Build();

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync(TestContext.Current.CancellationToken));

        var message = ex.ToString();
        Assert.Contains("Durable workflow [plain]", message);
        Assert.Contains(typeof(PlainInput).FullName!, message);
        Assert.Contains(typeof(PlainState).FullName!, message);
        Assert.Contains(typeof(PlainFact).FullName!, message);
        Assert.Contains(typeof(PlainOutcome).FullName!, message);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }
}
