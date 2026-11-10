using Akka.Durable.TestKit;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Xunit;
using Xunit.Sdk;

namespace Akka.Durable.Tests;

/// <summary>Pure decision tests for the calls an entity makes to a workflow. No ActorSystem.</summary>
public class EntityWorkflowCallDeciderSpecs
{
    private static readonly DateTimeOffset Due = new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly WorkflowId Campaign1 = new("campaign-test-entity");

    private static DeciderScenario<BillingState, BillingCommand, BillingEvent> Scenario(
        EntityDefinition<BillingState, BillingCommand, BillingEvent>? definition = null) =>
        DeciderScenario.For(definition ?? Billing.Definition);

    // The same decisions, but the definition does not name the campaign workflow.
    private static readonly EntityDefinition<BillingState, BillingCommand, BillingEvent> Undeclared =
        new(Billing.TypeName, _ => new BillingState(false, []), Billing.Decide, Billing.Evolve);

    private static BillingCommand Command(string name) => name switch
    {
        "start" => new IssueBill(Due),
        "start-watched" => new IssueBillWatched(Due),
        "notify" => new PayBill(),
        _ => new StopCampaign("written off"),
    };

    private static DecisionContext<BillingState, BillingCommand, BillingEvent> Context() =>
        new(new EntityId("test-entity"), Billing.TypeName, DateTimeOffset.UnixEpoch);

    [Theory]
    [InlineData("start")]
    [InlineData("start-watched")]
    public void A_start_with_or_without_then_is_asserted_by_value(string command)
    {
        Scenario().When(Command(command))
            .ThenPersisted(new BillIssued())
            .ThenStarted(Campaign.Definition, Campaign1,
                new Invoice("test-entity", Due, TimeSpan.FromDays(7), TimeSpan.FromDays(14), 3));
    }

    [Theory]
    [InlineData("campaign-other", "test-entity", 3, 2100)]
    [InlineData("campaign-test-entity", "other", 3, 2100)]
    [InlineData("campaign-test-entity", "test-entity", 4, 2100)]
    [InlineData("campaign-test-entity", "test-entity", 3, 2101)]
    public void ThenStarted_fails_when_the_id_or_the_input_differs(string id, string invoice, int reminders, int year)
    {
        var outcome = Scenario().When(Command("start"));

        Assert.ThrowsAny<XunitException>(() => outcome.ThenStarted(Campaign.Definition, new WorkflowId(id),
            new Invoice(invoice, new DateTimeOffset(year, 1, 1, 0, 0, 0, TimeSpan.Zero), TimeSpan.FromDays(7), TimeSpan.FromDays(14), reminders)));
    }

    [Fact]
    public void ThenStarted_fails_for_a_workflow_type_of_another_name()
    {
        var other = Campaign.Definition with { TypeName = new WorkflowTypeName("other") };

        Assert.ThrowsAny<XunitException>(() => Scenario().When(Command("start")).ThenStarted(other, Campaign1,
            new Invoice("test-entity", Due, TimeSpan.FromDays(7), TimeSpan.FromDays(14), 3)));
    }

    [Fact]
    public void A_notify_is_asserted_by_value()
    {
        Scenario().Given(new BillIssued()).When(Command("notify")).ThenNotified(Campaign.Definition, Campaign1, new Paid());
    }

    [Theory]
    [InlineData("campaign-other", false)]
    [InlineData("campaign-test-entity", true)]
    public void ThenNotified_fails_when_the_id_or_the_fact_differs(string id, bool otherFact)
    {
        var outcome = Scenario().Given(new BillIssued()).When(Command("notify"));
        CampaignFact fact = otherFact ? new StopRequested("x", "y") : new Paid();

        Assert.ThrowsAny<XunitException>(() => outcome.ThenNotified(Campaign.Definition, new WorkflowId(id), fact));
    }

    [Fact]
    public void A_cancel_is_asserted_by_value()
    {
        Scenario().Given(new BillIssued()).When(new StopCampaign("written off"))
            .ThenCancelled(Campaign.Definition, Campaign1, "written off", "billing");
    }

    [Theory]
    [InlineData("campaign-other", "written off", "billing")]
    [InlineData("campaign-test-entity", "other reason", "billing")]
    [InlineData("campaign-test-entity", "written off", "someone")]
    public void ThenCancelled_fails_when_the_id_the_reason_or_the_caller_differs(string id, string reason, string by)
    {
        var outcome = Scenario().Given(new BillIssued()).When(new StopCampaign("written off"));

        Assert.ThrowsAny<XunitException>(() => outcome.ThenCancelled(Campaign.Definition, new WorkflowId(id), reason, by));
    }

    [Fact]
    public void An_assertion_for_one_kind_of_call_does_not_match_another_kind()
    {
        var started = Scenario().When(Command("start"));

        Assert.ThrowsAny<XunitException>(() => started.ThenNotified(Campaign.Definition, Campaign1, new Paid()));
        Assert.ThrowsAny<XunitException>(() => started.ThenCancelled(Campaign.Definition, Campaign1, "x", "billing"));
    }

    [Theory]
    [InlineData("start")]
    [InlineData("start-watched")]
    [InlineData("notify")]
    [InlineData("cancel")]
    public void A_decision_that_calls_a_workflow_the_definition_does_not_name_fails_When(string command)
    {
        var failure = Assert.ThrowsAny<XunitException>(() => Scenario(Undeclared).When(Command(command)));

        Assert.Contains("campaigns", failure.Message);
    }

    [Fact]
    public void A_decision_without_calls_passes_for_a_definition_that_names_no_workflow()
    {
        Scenario(Undeclared).When(new CampaignAnswered("Started")).ThenPersisted(new BillingTold("Started"));
    }

    [Fact]
    public void A_call_to_a_workflow_type_of_another_name_fails_When_even_with_the_same_four_types()
    {
        var other = Campaign.Definition with { TypeName = new WorkflowTypeName("other") };
        var definition = Billing.Definition with
        {
            Decide = (_, _, ctx) => ctx.Persist(new BillIssued())
                .Start(other, Campaign1, Billing.InputOf(ctx.EntityId, Due)),
        };

        Assert.ThrowsAny<XunitException>(() => Scenario(definition).When(Command("start")));
    }

    private static Handled<BillingCommand, BillingEvent> ResumeStart(
        BillingState state, DecisionContext<BillingState, BillingCommand, BillingEvent> ctx) =>
        ctx.None().Start(Campaign.Definition, Billing.CampaignOf(ctx.EntityId), Billing.InputOf(ctx.EntityId, Due));

    [Fact]
    public void A_start_issued_from_Resume_is_asserted_after_Resumed()
    {
        Scenario(Billing.Definition with { Resume = ResumeStart }).Given(new BillIssued()).Resumed()
            .ThenStarted(Campaign.Definition, Campaign1, new Invoice("test-entity", Due, TimeSpan.FromDays(7), TimeSpan.FromDays(14), 3));
    }

    [Fact]
    public void A_call_from_Resume_to_a_workflow_the_definition_does_not_name_fails_Resumed()
    {
        var definition = Undeclared with { Resume = ResumeStart };

        Assert.ThrowsAny<XunitException>(() => Scenario(definition).Given(new BillIssued()).Resumed());
    }

    [Theory]
    [InlineData("", "billing")]
    [InlineData("reason", "")]
    public void Cancel_rejects_an_empty_reason_or_caller(string reason, string by)
    {
        Assert.ThrowsAny<ArgumentException>(() => Context().None().Cancel(
            Campaign.Definition, Campaign1, reason, by,
            then: _ => new CampaignAnswered("x"), gaveUp: g => new CampaignUnreachable(g.Reason)));
    }

    private static readonly Invoice Input = new("a", Due, TimeSpan.FromDays(7), TimeSpan.FromDays(14), 3);
    private static readonly WorkflowTypeName Type = new("campaigns");

    [Fact]
    public void The_effect_records_compare_by_value()
    {
        var start = new StartWorkflow(Type, new WorkflowId("w"), Input);
        var same = new StartWorkflow(Type, new WorkflowId("w"), Input with { });
        Assert.Equal(start, same);
        Assert.Equal(start.GetHashCode(), same.GetHashCode());
        Assert.NotEqual(start, new StartWorkflow(Type, new WorkflowId("v"), Input));
        Assert.NotEqual(start, new StartWorkflow(Type, new WorkflowId("w"), Input with { Reminders = 4 }));
        Assert.NotEqual(start, new StartWorkflow(new WorkflowTypeName("other"), new WorkflowId("w"), Input));

        var notify = new NotifyWorkflow(Type, new WorkflowId("w"), new StopRequested("r", "b"));
        Assert.Equal(notify, new NotifyWorkflow(Type, new WorkflowId("w"), new StopRequested("r", "b")));
        Assert.Equal(notify.GetHashCode(), new NotifyWorkflow(Type, new WorkflowId("w"), new StopRequested("r", "b")).GetHashCode());
        Assert.NotEqual(notify, new NotifyWorkflow(Type, new WorkflowId("w"), new StopRequested("r", "c")));

        var cancel = new CancelWorkflow(Type, new WorkflowId("w"), "r", "b");
        Assert.Equal(cancel, new CancelWorkflow(Type, new WorkflowId("w"), "r", "b"));
        Assert.Equal(cancel.GetHashCode(), new CancelWorkflow(Type, new WorkflowId("w"), "r", "b").GetHashCode());
        Assert.NotEqual(cancel, new CancelWorkflow(Type, new WorkflowId("w"), "r", "c"));
        Assert.NotEqual(cancel, new CancelWorkflow(Type, new WorkflowId("w"), "s", "b"));
    }
}
