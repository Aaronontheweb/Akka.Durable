using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Event;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>Keeps the warnings and errors the actor system logs, so a spec can read them whole.</summary>
internal sealed class LogSink : ReceiveActor
{
    public LogSink(ConcurrentQueue<LogEvent> sink) => Receive<LogEvent>(sink.Enqueue);
}

internal static class LogCollector
{
    public static ConcurrentQueue<LogEvent> Attach(ActorSystem system)
    {
        var sink = new ConcurrentQueue<LogEvent>();
        var collector = system.ActorOf(Props.Create(() => new LogSink(sink)));
        system.EventStream.Subscribe(collector, typeof(Warning));
        system.EventStream.Subscribe(collector, typeof(Error));
        return sink;
    }

    public static string Text(LogEvent e) => $"{e.Message} {e.Cause}";
}

/// <summary>
/// An effect with no registered handler is a deployment mistake: it is logged at error level, with the effect's
/// type and the interface to register but never the effect's contents. A handler that throws is a different case.
/// </summary>
public abstract class WorkflowMissingHandlerLogSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private const string Secret = "order-7f3a91c2";
    private const string Interface = "IEffectHandler<ChargeCard, CheckoutFact>";

    // The stock checkout, but the give-up fact keeps the error, so a spec can read it from the workflow's state.
    protected override WorkflowDefinition<PlaceOrder, CheckoutState, CheckoutFact, CheckoutOutcome> Definition =>
        Checkout.Definition with { GaveUp = f => new WorkStuck($"{f.What}: {f.Error}", f.Reason) };

    // Every handler except the one for ChargeCard.
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services
            .AddSingleton<CheckoutServices>()
            .AddScoped<IEffectHandler<ReserveStock, CheckoutFact>, ReserveStockHandler>()
            .AddScoped<IEffectHandler<UndoOrder, CheckoutFact>, UndoOrderHandler>()
            .AddScoped<IEffectHandler<OpenSupportTicket, CheckoutFact>, OpenSupportTicketHandler>();

    // The workflow's table sends given-up work to NeedsAttention, whose reason reads "<what>: <error> <GaveUpReason>".
    private async Task<string> GaveUp(string id, PlaceOrder order)
    {
        await CheckoutRef(id).Start(order, AskCancellation());
        var running = await SettledIn<NeedsAttention>(id);
        return Assert.IsType<NeedsAttention>(((CheckoutState)running.State).Value).Why;
    }

    [Fact]
    public async Task A_missing_handler_is_logged_as_an_error_and_the_work_gives_up()
    {
        var log = LogCollector.Attach(Sys);

        var why = await GaveUp("w1", new PlaceOrder(Secret, 100));

        Assert.StartsWith("ChargeCard: ", why);
        Assert.Contains(Interface, why);
        Assert.EndsWith(" Failed", why);
        Assert.Single(Services.Issued.OfType<ReserveStock>()); // the reservation ran once: the workflow did not restart

        // The event stream delivers to the collector a moment after the entity logs.
        await AwaitAssertAsync(() => Assert.Contains(log, e => LogCollector.Text(e).Contains("ChargeCard")),
            cancellationToken: TestCancellation);
        var entry = Assert.Single(log, e => LogCollector.Text(e).Contains("ChargeCard"));
        Assert.IsType<Error>(entry);
        Assert.Contains(Interface, LogCollector.Text(entry));
        Assert.Contains("checkouts", LogCollector.Text(entry));
        Assert.Contains("w1", LogCollector.Text(entry));
        Assert.DoesNotContain(log, e => LogCollector.Text(e).Contains(Secret));
    }

    [Fact]
    public async Task A_handler_that_throws_gives_up_the_same_way_but_is_not_logged_as_a_missing_handler()
    {
        Services.Reserve = (_, _) => throw new InvalidOperationException("boom-from-handler");
        var log = LogCollector.Attach(Sys);

        var why = await GaveUp("w2", new PlaceOrder("o2", 100));

        Assert.Contains("boom-from-handler", why);
        Assert.EndsWith(" Failed", why);
        await AwaitAssertAsync(() => Assert.Contains(log, e => e is Warning && LogCollector.Text(e).Contains("boom-from-handler")),
            cancellationToken: TestCancellation);
        Assert.DoesNotContain(log, e => LogCollector.Text(e).Contains("No effect handler", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(log, e => e is Error);
    }
}

public sealed class LocalWorkflowMissingHandlerLogSpecs(ITestOutputHelper output) : WorkflowMissingHandlerLogSpecs(output);

public sealed class ShardedWorkflowMissingHandlerLogSpecs(ITestOutputHelper output) : WorkflowMissingHandlerLogSpecs(output)
{
    protected override bool Sharded => true;
}

public abstract class EntityMissingHandlerLogSpecs(ITestOutputHelper output) : CounterHostingSpec(output)
{
    private const string Secret = "note-5b1e77d0";

    // Unregistered has no handler. Explode has one, and it throws.
    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
            .AddScoped<IEffectHandler<Explode, CounterCommand>, ExplodeHandler>();

    [Fact]
    public async Task Every_missing_handler_is_logged_as_an_error_without_the_effects_contents()
    {
        var log = LogCollector.Attach(Sys);

        CounterRef("m1").Tell(new CounterCommand(new NoHandler(Secret)));
        CounterRef("m1").Tell(new CounterCommand(new NoHandler(Secret)));

        // One entry for each occurrence names the effect type, the interface to register and the entity.
        await AwaitAssertAsync(() => Assert.True(log.Count(e => e is Error
            && LogCollector.Text(e) is var text && text.Contains("IEffectHandler<Unregistered, CounterCommand>") && text.Contains("counter")
            && text.Contains("m1")) >= 2), cancellationToken: TestCancellation);

        Assert.DoesNotContain(log, e => LogCollector.Text(e).Contains(Secret));
    }

    [Fact]
    public async Task A_handler_that_throws_is_logged_as_a_warning_and_not_as_a_missing_handler()
    {
        var log = LogCollector.Attach(Sys);

        await CounterRef("m2").Ask(new ExplodeLater(), AskCancellation());

        await AwaitAssertAsync(() => Assert.Contains(log, e => e is Warning && LogCollector.Text(e).Contains("boom")),
            cancellationToken: TestCancellation);
        Assert.DoesNotContain(log, e => e is Error);
        Assert.DoesNotContain(log, e => LogCollector.Text(e).Contains("No effect handler", StringComparison.OrdinalIgnoreCase));
    }
}

public sealed class LocalEntityMissingHandlerLogSpecs(ITestOutputHelper output) : EntityMissingHandlerLogSpecs(output)
{
    protected override ShardedHosting? Sharding => null;
}

public sealed class ShardedEntityMissingHandlerLogSpecs(ITestOutputHelper output) : EntityMissingHandlerLogSpecs(output)
{
    protected override ShardedHosting? Sharding => new();

    protected override void ConfigureCluster(AkkaConfigurationBuilder builder) => ConfigureSingleNodeCluster(builder);
}
