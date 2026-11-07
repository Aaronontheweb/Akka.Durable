using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.TestKit;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>
/// The local host passivates entities that have been idle for <see cref="EntityOptions.PassivateIdleAfter"/>.
/// </summary>
public class LocalIdlePassivationSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    // Long enough that a CI stall between keep-alive messages can't look like idleness,
    // short enough to keep the tests quick.
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(1);

    // The host checks every IdleTimeout / 2, so an idle entity stops within 1.5x the timeout.
    private static readonly TimeSpan PassivationBound = TimeSpan.FromSeconds(15);

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
    }

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder.WithCustomSerializer(CounterSerializer.CreateSerializerDetails).WithDurableEntity(Counter.Definition with
        {
            Options = EntityOptions.Default with { PassivateIdleAfter = IdleTimeout },
        });

    private IEntityRef<CounterCommand> CounterRef(string id) => EntityRef<CounterCommand>(id);

    private async Task<int> CountOf(IEntityRef<CounterCommand> counter) =>
        (await counter.Ask<CountIs>(new GetCount(), AskCancellation())).Count;

    private Task<IActorRef> EntityActorOf(string id) =>
        Sys.ActorSelection($"/user/durable-counter/{Uri.EscapeDataString(id)}")
            .ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);

    [Fact]
    public async Task Idle_entity_is_passivated_after_the_timeout()
    {
        await CounterRef("a").Ask<Accepted>(new Increment(1), AskCancellation());
        var entity = await EntityActorOf("a");
        Watch(entity);

        await ExpectTerminatedAsync(entity, PassivationBound, cancellationToken: TestCancellation);
    }

    [Fact]
    public async Task Next_message_reactivates_a_passivated_entity_and_recovers_its_state()
    {
        var counter = CounterRef("a");
        await counter.Ask<Accepted>(new Increment(3), AskCancellation());
        var entity = await EntityActorOf("a");
        Watch(entity);
        await ExpectTerminatedAsync(entity, PassivationBound, cancellationToken: TestCancellation);

        Assert.Equal(3, await CountOf(counter));
        Assert.NotEqual(entity, await EntityActorOf("a"));
    }

    [Fact]
    public async Task Entity_that_keeps_receiving_messages_is_not_passivated()
    {
        var counter = CounterRef("a");
        await counter.Ask<Accepted>(new Increment(1), AskCancellation());
        var entity = await EntityActorOf("a");
        Watch(entity);

        // Several timeouts' worth of traffic, far more often than the timeout.
        var until = DateTime.UtcNow + IdleTimeout * 3;
        while (DateTime.UtcNow < until)
        {
            Assert.Equal(1, await CountOf(counter));
            await Task.Delay(IdleTimeout / 10, TestCancellation);
        }

        await ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100), TestCancellation);
        Assert.Equal(entity, await EntityActorOf("a"));
    }

    [Fact]
    public async Task Messages_sent_while_an_idle_entity_passivates_are_delivered_to_its_next_incarnation()
    {
        var counter = CounterRef("a");
        await counter.Ask<Accepted>(new Increment(1), AskCancellation());
        var entity = await EntityActorOf("a");
        Watch(entity);

        // Hold the entity inside a Run effect: it stashes the host's stop message until the gate
        // opens, which keeps the host's passivation window open for as long as the test needs.
        counter.Tell(new WaitForGate(), TestActor);

        // Well past the timeout, so the host has started passivating and buffers what follows.
        await Task.Delay(IdleTimeout * 3, TestCancellation);
        const int buffered = 20;
        for (var i = 0; i < buffered; i++) counter.Tell(new Increment(1), TestActor);

        Host.Services.GetRequiredService<Gate>().Open();

        // The old incarnation stops; the buffered increments reach the new one. Terminated and the
        // new incarnation's replies come from different senders, so don't depend on their order.
        var received = await ReceiveNAsync(2 + buffered, cancellationToken: TestCancellation)
            .ToListAsync(TestCancellation);
        Assert.Single(received.OfType<GateOpened>());
        Assert.Contains(received.OfType<Terminated>(), t => t.ActorRef.Equals(entity));
        Assert.Equal(buffered, received.OfType<Accepted>().Count());
        Assert.Equal(1 + buffered, await CountOf(counter));
    }
}

/// <summary>
/// With idle passivation disabled the local host leaves idle entities running.
/// </summary>
public class LocalIdlePassivationDisabledSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<EffectRecorder>();
        services.AddSingleton<Gate>();
        services.AddScoped<ScopedRecorder>();
    }

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder.WithCustomSerializer(CounterSerializer.CreateSerializerDetails).WithDurableEntity(Counter.Definition with
        {
            Options = EntityOptions.Default with { PassivateIdleAfter = TimeSpan.Zero },
        });

    [Fact]
    public async Task Idle_entity_stays_alive()
    {
        await EntityRef<CounterCommand>("a").Ask<Accepted>(new Increment(1), AskCancellation());
        var entity = await Sys.ActorSelection("/user/durable-counter/a")
            .ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);
        Watch(entity);

        await ExpectNoMsgAsync(TimeSpan.FromSeconds(2), TestCancellation);
        Assert.Equal(1, (await EntityRef<CounterCommand>("a").Ask<CountIs>(new GetCount(), AskCancellation())).Count);
    }
}
