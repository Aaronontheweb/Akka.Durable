using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Campaigns;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// With <c>VerifyMessageSerialization</c> on, every message is serialized and read back, including the journal's
/// internal reply to a persist. Entities and workflows whose events are C# unions must keep their values through
/// persisting, restarting and recovering.
/// </summary>
public sealed class VerifiedSerializationEntitySpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    protected override ShardedHosting? Sharding => null;

    protected override bool VerifyMessageSerialization => true;

    [Fact]
    public async Task Union_events_survive_several_persists_a_restart_and_recovery()
    {
        var counter = DurableCounterRef("v1");
        await counter.Ask(new Increment(2), AskCancellation());
        await counter.Ask(new Increment(3), AskCancellation());
        await counter.Ask(new LockCounter(), AskCancellation());

        Assert.Equal(new CounterState(5, true), (await counter.GetStateAsync(AskCancellation())).State);

        await counter.Ask(new Shutdown(), AskCancellation());

        // The next read activates a new actor, which recovers from the journal.
        var recovered = await counter.GetStateAsync(AskCancellation());
        Assert.Equal(new CounterState(5, true), recovered.State);
        Assert.True(recovered.SequenceNr >= 3);
    }
}

public abstract class VerifiedSerializationWorkflowSpecs(ITestOutputHelper output) : CampaignWorkflowKit(output)
{
    protected override bool VerifyMessageSerialization => true;

    [Fact]
    public async Task A_workflow_with_union_events_keeps_its_values_through_persists_a_restart_and_recovery()
    {
        Services.Send = (_, _) => Task.FromResult<CampaignFact>(new ReminderSent());
        var longTime = TimeSpan.FromHours(1);
        var campaign = CampaignRef("v1");
        await campaign.Start(
            new Invoice("v1", DateTimeOffset.UtcNow + longTime, longTime, longTime, Reminders: 2), AskCancellation());
        await campaign.Notify(new SendNow(), AskCancellation());

        await AwaitRunning("v1", running => Assert.Equal(new Pausing(1), StateOf(running)));

        await RestartWorkflow("v1");

        await AwaitRunning("v1", running => Assert.Equal(new Pausing(1), StateOf(running)));
    }
}

public sealed class LocalVerifiedSerializationWorkflowSpecs(ITestOutputHelper output)
    : VerifiedSerializationWorkflowSpecs(output)
{
    protected override bool Sharded => false;
}

public sealed class ShardedVerifiedSerializationWorkflowSpecs(ITestOutputHelper output)
    : VerifiedSerializationWorkflowSpecs(output)
{
    protected override bool Sharded => true;
}
