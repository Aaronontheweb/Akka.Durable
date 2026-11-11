using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Hosting;
using Akka.Reminders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>The host checks at startup that the workflows an entity uses are hosted, whatever the order.</summary>
public class EntityUsesWorkflowStartupSpecs
{
    private static IHost Build(
        bool workflowFirst, WorkflowDefinition<Invoice, CampaignState, CampaignFact, CampaignOutcome> hosted, bool reminders = true) =>
        new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddAkka("uses-startup", builder =>
                {
                    builder.WithCustomSerializer(CampaignSerializer.CreateSerializerDetails)
                        .WithCustomSerializer(BillingSerializer.CreateSerializerDetails);
                    if (reminders) builder.WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)));
                    if (workflowFirst) builder.WithDurableWorkflow(hosted).WithDurableEntity(Billing.Definition);
                    else builder.WithDurableEntity(Billing.Definition).WithDurableWorkflow(hosted);
                });
            })
            .Build();

    [Fact]
    public async Task A_workflow_registered_before_the_entity_that_uses_it_starts()
    {
        using var host = Build(workflowFirst: true, Campaign.Definition);

        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task An_entity_that_uses_a_workflow_fails_startup_without_Akka_Reminders()
    {
        using var host = Build(workflowFirst: false, Campaign.Definition, reminders: false);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync(TestContext.Current.CancellationToken));

        var message = ex.ToString();
        Assert.Contains("Durable entity [billing] cannot start: it calls workflows (Uses), and Akka.Reminders is not configured", message);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_workflow_of_the_same_four_types_hosted_under_another_name_fails_startup_and_names_both(bool workflowFirst)
    {
        using var host = Build(workflowFirst, Campaign.Definition with { TypeName = new WorkflowTypeName("campaigns-v2") });

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync(TestContext.Current.CancellationToken));

        var message = ex.ToString();
        Assert.Contains("[billing]", message);
        Assert.Contains("[campaigns]", message);
        Assert.Contains("[campaigns-v2]", message);

        await host.StopAsync(TestContext.Current.CancellationToken);
    }
}
