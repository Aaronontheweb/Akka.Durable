using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Akka.Hosting;
using Akka.Reminders;
using Akka.Reminders.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// Calls from an entity to a workflow when the entity is stopped: the wake brings it back with nobody touching it.
/// The effect time-out is long, so a call that the kill cuts off is still in flight at the kill.
/// </summary>
public abstract class OutboxCallsSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    private static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(6);

    // The wake of a call without a policy is due at time-out + 5 s, plus room for a slow machine.
    private static readonly TimeSpan Patience = CallTimeout + TimeSpan.FromSeconds(5) + TimeSpan.FromSeconds(20);

    protected override EntityOptions Options => EntityOptions.Default with { EffectTimeout = CallTimeout };

    protected SwitchableReminderStorage ReminderStore { get; } = new();

    protected override IReminderStorage CreateReminderStorage(ActorSystem system) => ReminderStore;

    private async Task Kill(string bill)
    {
        var entity = await EntityActor(bill);
        Watch(entity);
        entity.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(entity, cancellationToken: TestCancellation);
    }

    /// <summary>Waits until the entity runs again, without sending it anything.</summary>
    private Task AwaitWoken(string bill) =>
        AwaitAssertAsync(async () => await EntityActor(bill), Patience, cancellationToken: TestCancellation);

    [Fact]
    public async Task A_start_recorded_and_cut_off_right_after_the_write_is_delivered_after_the_wake_with_nobody_touching_the_entity()
    {
        var gate = Calls.Hold("start"); // the first attempt starts and goes no further
        await Issue("woken");
        await AwaitAttempts("start", 1);
        var wake = Assert.Single(await RemindersOf("woken"));
        Assert.Equal(new ReminderKey("durable-effects-wake"), wake.Key); // and no reminder of its own for the start
        await Kill("woken");
        gate.SetResult();

        // Nothing asks for the entity. The wake starts it, and it delivers the start again.
        await AwaitCampaign<WorkflowRunning<CampaignState>>("woken");

        await AwaitNoReminders("woken");
        Assert.Equal(2, Calls.Count("start"));
        Assert.Equal(1, WorkflowStarts);
    }

    [Theory]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task A_notify_or_cancel_cut_off_by_a_stop_is_given_up_as_interrupted_through_the_uses_mapping_and_not_sent_again(string kind)
    {
        await Issue("cut-off");
        await AwaitCampaign<WorkflowRunning<CampaignState>>("cut-off");
        Calls.Hold(kind); // the call never completes
        if (kind == "notify") await Bill("cut-off").Ask(new PayBill(), AskCancellation(FirstAsk));
        else await Bill("cut-off").Ask(new StopCampaign("stop"), AskCancellation(FirstAsk));
        await AwaitAttempts(kind, 1);
        await Kill("cut-off");

        await AwaitWoken("cut-off");

        await AwaitAnswers("cut-off", "gave up: Interrupted");
        await AwaitNoReminders("cut-off");
        await LetTimePass();
        Assert.Equal(1, Calls.Count(kind));
        Assert.Empty(await PendingEffects<BillingCommand>("cut-off"));
    }

    [Fact]
    public async Task When_the_wake_cannot_be_stored_the_issuing_command_fails_and_writes_nothing_and_a_later_one_starts_the_workflow()
    {
        ReminderStore.Failing = true;

        await Assert.ThrowsAnyAsync<Exception>(() => Issue("refused"));

        ReminderStore.Failing = false;
        Assert.False((await Bill("refused").GetStateAsync(AskCancellation(FirstAsk))).State.Issued);
        Assert.Empty(Calls.Attempts);
        Assert.Empty(await PendingEffects<BillingCommand>("refused"));
        Assert.IsType<WorkflowNotStarted>((await CampaignOf("refused")).Value);

        await Issue("refused");

        await AwaitCampaign<WorkflowRunning<CampaignState>>("refused");
        Assert.True((await Bill("refused").GetStateAsync(AskCancellation())).State.Issued);
        Assert.Equal(1, WorkflowStarts);
    }
}

public sealed class LocalOutboxCallsSpecs(ITestOutputHelper output) : OutboxCallsSpecs(output);

public sealed class ShardedOutboxCallsSpecs(ITestOutputHelper output) : OutboxCallsSpecs(output)
{
    protected override bool Sharded => true;
}

/// <summary>An entity that declares the workflow with <c>Uses(workflow)</c> only may start it and nothing else.</summary>
public sealed class LocalStartOnlyUsesSpecs(ITestOutputHelper output) : EntityCallsKit(output)
{
    protected override EntityDefinition<BillingState, BillingCommand, BillingEvent> Entity =>
        new EntityDefinition<BillingState, BillingCommand, BillingEvent>(
            Billing.TypeName, _ => new BillingState(false, []), Billing.Decide, Billing.Evolve).Uses(Campaign.Definition);

    [Theory]
    [InlineData("notify")]
    [InlineData("cancel")]
    public async Task A_notify_or_cancel_is_logged_and_dropped_but_a_start_is_delivered(string kind)
    {
        var log = Errors();
        await Issue("start-only"); // the start is allowed
        await AwaitCampaign<WorkflowRunning<CampaignState>>("start-only");
        await AwaitNoReminders("start-only");

        if (kind == "notify") await Bill("start-only").Ask(new PayBill(), AskCancellation());
        else await Bill("start-only").Ask(new StopCampaign("stop"), AskCancellation());

        await AwaitAssertAsync(() => Assert.NotEmpty(ErrorsAbout(log, "campaigns")), cancellationToken: TestCancellation);
        await LetTimePass();
        Assert.Single(ErrorsAbout(log, "campaigns"));
        Assert.Equal(0, Calls.Count(kind));
        Assert.Equal(1, Calls.Count("start"));
        Assert.Empty(await PendingEffects<BillingCommand>("start-only"));
        Assert.Empty(await RemindersOf("start-only")); // nothing was journaled, so no wake
        Assert.Empty((await Bill("start-only").GetStateAsync(AskCancellation())).State.Answers);
        Assert.IsType<WorkflowRunning<CampaignState>>((await CampaignOf("start-only")).Value);
    }
}

/// <summary>An entity with effect handlers cannot start without Akka.Reminders, with or without a retry policy.</summary>
public sealed class OutboxStartupSpecs
{
    public static TheoryData<bool> Policies => new() { false, true };

    [Theory]
    [MemberData(nameof(Policies))]
    public async Task An_entity_with_an_effect_handler_fails_startup_without_Akka_Reminders(bool withPolicy)
    {
        var ct = TestContext.Current.CancellationToken;
        using var host = new HostBuilderBuilder().Build(withPolicy);

        var failure = await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync(ct));

        var text = failure.ToString();
        Assert.Contains("Durable entity [counter] cannot start", text);
        Assert.Contains("Akka.Reminders is not configured", text);
        Assert.Contains("Probe", text);
        await host.StopAsync(ct);
    }

    [Theory]
    [MemberData(nameof(Policies))]
    public async Task The_same_host_starts_when_Akka_Reminders_is_configured(bool withPolicy)
    {
        var ct = TestContext.Current.CancellationToken;
        using var host = new HostBuilderBuilder().Build(withPolicy, reminders: true);

        await host.StartAsync(ct);

        await host.StopAsync(ct);
    }

    private sealed class HostBuilderBuilder
    {
        public IHost Build(bool withPolicy, bool reminders = false) =>
            new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddDurableEntities();
                    services.AddSingleton(new ProbeWorld());
                    services.AddEffectHandler<Probe, CounterCommand, ProbeHandler>(
                        withPolicy ? Retry.Every(TimeSpan.FromSeconds(1), attempts: 3) : null);
                    services.AddAkka("outbox-startup", builder =>
                    {
                        if (reminders) builder.WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)));
                        builder
                            .WithCustomSerializer(CounterSerializer.CreateSerializerDetails)
                            .WithCustomSerializer(ProbeSerializer.CreateSerializerDetails)
                            .WithDurableEntity(Counter.Definition);
                    });
                })
                .Build();
    }
}
