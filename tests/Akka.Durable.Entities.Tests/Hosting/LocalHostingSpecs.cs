using Akka.Actor;
using Akka.Cluster.Hosting;
using Akka.Durable.Entities.Tests.Fixtures;
using Xunit;

namespace Akka.Durable.Entities.Tests.Hosting;

/// <summary>
/// Entities hosted in-process by the local child-per-entity host, addressed through IEntityRef.
/// </summary>
public class LocalHostingSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    protected override ShardOptions? Sharding => null;

    [Fact]
    public async Task Ask_through_an_entity_ref_works_from_outside_an_actor()
    {
        var counter = CounterRef("a");

        await counter.Ask<Accepted>(new Increment(2), AskCancellation());

        Assert.Equal(2, await CountOf(counter));
    }

    [Fact]
    public async Task One_entity_per_id_and_distinct_ids_are_independent()
    {
        var a = CounterRef("a");
        var b = CounterRef("b");

        await a.Ask<Accepted>(new Increment(1), AskCancellation());
        await a.Ask<Accepted>(new Increment(1), AskCancellation());
        await b.Ask<Accepted>(new Increment(5), AskCancellation());

        Assert.Equal(2, await CountOf(a));
        Assert.Equal(5, await CountOf(b));

        // Exactly one child actor per entity id under the host.
        Sys.ActorSelection("/user/durable-counter/*").Tell(new Identify("children"), TestActor);
        var children = await ReceiveNAsync(2, cancellationToken: TestCancellation).ToListAsync(TestCancellation);
        await ExpectNoMsgAsync(NoMsgWindow, TestCancellation);
        Assert.All(children, c => Assert.IsType<ActorIdentity>(c));
    }

    [Fact]
    public async Task Entity_ids_that_are_not_valid_actor_names_still_route()
    {
        var counter = CounterRef("tenant/42 #1");

        await counter.Ask<Accepted>(new Increment(3), AskCancellation());

        Assert.Equal(3, await CountOf(counter));
    }

    [Fact]
    public async Task Stopped_entity_is_reactivated_by_the_next_message_and_recovers_state()
    {
        var counter = CounterRef("a");
        await counter.Ask<Accepted>(new Increment(3), AskCancellation());

        await counter.Ask<Accepted>(new Shutdown(), AskCancellation());

        Assert.Equal(3, await CountOf(counter));
    }

    [Fact]
    public Task Using_an_entity_immediately_after_it_stops_loses_no_messages() =>
        AssertStopThenUseLosesNothing(CounterRef("a"), rounds: 25);

    [Fact]
    public async Task Messages_sent_while_an_entity_passivates_are_delivered_to_its_next_incarnation()
    {
        var counter = CounterRef("a");
        await counter.Ask<Accepted>(new Increment(1), AskCancellation());

        counter.Tell(new Shutdown(), TestActor);
        for (var i = 0; i < 20; i++) counter.Tell(new Increment(1), TestActor);

        for (var i = 0; i < 21; i++) await ExpectMsgAsync<Accepted>(cancellationToken: TestCancellation);
        Assert.Equal(21, await CountOf(counter));
    }

    [Fact]
    public async Task Per_sender_ordering_is_preserved()
    {
        // Interleaving commands with queries makes any reordering visible in the replies.
        var counter = CounterRef("a");
        const int rounds = 200;
        for (var i = 0; i < rounds; i++)
        {
            counter.Tell(new Increment(1), TestActor);
            counter.Tell(new GetCount(), TestActor);
        }

        for (var i = 1; i <= rounds; i++)
        {
            await ExpectMsgAsync<Accepted>(cancellationToken: TestCancellation);
            await ExpectMsgAsync<CountIs>(c => c.Count == i, cancellationToken: TestCancellation);
        }
    }

    [Fact]
    public async Task Run_effects_resolve_services_from_the_host_container()
    {
        await CounterRef("a").Ask<Accepted>(new IncrementAndRecord(1), AskCancellation());

        Assert.Equal(["run:1", "disposed"], Recorder.Entries);
    }

    [Fact]
    public async Task Reminders_are_delivered_to_the_entity_through_its_host()
    {
        var counter = CounterRef("a");

        // Schedules a reminder whose payload is Increment(1).
        counter.Tell(new RemindLater(TimeSpan.FromMilliseconds(200)));

        await AwaitAssertAsync(
            async () => Assert.Equal(1, await CountOf(counter)),
            duration: TimeSpan.FromSeconds(15),
            interval: TimeSpan.FromMilliseconds(250),
            cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task Untyped_Tell_accepts_the_command_type_and_rejects_anything_else()
    {
        ICanTell counter = CounterRef("a");

        var ex = Assert.Throws<ArgumentException>(() => counter.Tell(new Increment(1), TestActor));
        Assert.Contains(nameof(CounterCommand), ex.Message);

        counter.Tell((CounterCommand)new Increment(1), TestActor);
        await ExpectMsgAsync<Accepted>(cancellationToken: TestCancellation);
    }
}
