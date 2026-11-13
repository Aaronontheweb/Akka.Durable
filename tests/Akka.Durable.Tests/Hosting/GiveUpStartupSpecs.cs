using Akka.Actor;
using Akka.Durable.Tests.Fixtures;
using Akka.Durable.Tests.Fixtures.Checkouts;
using Akka.Event;
using Akka.Hosting;
using Akka.Reminders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>Starts a host the way an application would, and reports whether it started.</summary>
internal static class StartupProbe
{
    /// <summary>The failure of the start, or null when the host started. The host is stopped either way.</summary>
    public static async Task<Exception?> Start(
        Action<IServiceCollection> handlers, Action<AkkaConfigurationBuilder> akka, bool reminders = true)
    {
        var ct = TestContext.Current.CancellationToken;
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                handlers(services);
                services.AddAkka("startup-probe", builder =>
                {
                    if (reminders)
                        builder.WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)));
                    akka(builder);
                });
            })
            .Build();

        try
        {
            await host.StartAsync(ct);
            return null;
        }
        catch (Exception failure)
        {
            return failure;
        }
        finally
        {
            await host.StopAsync(ct);
        }
    }

    /// <summary>The message of the failure and of every failure inside it.</summary>
    public static IEnumerable<string> Messages(Exception? failure)
    {
        if (failure is null) yield break;
        yield return failure.Message;
        var inner = failure is AggregateException aggregate ? aggregate.InnerExceptions : [failure.InnerException!];
        foreach (var message in inner.Where(e => e is not null).SelectMany(Messages)) yield return message;
    }

    public static void AssertStarted(Exception? failure) =>
        Assert.True(failure is null, $"The host did not start: {failure}");

    public static AkkaConfigurationBuilder HostCounter(
        AkkaConfigurationBuilder builder, EntityDefinition<CounterState, CounterCommand, CounterEvent> definition) =>
        builder.WithCustomSerializer(CounterSerializer.CreateSerializerDetails).WithDurableEntity(definition);

    public static AkkaConfigurationBuilder HostCheckouts(AkkaConfigurationBuilder builder) =>
        builder.WithCustomSerializer(CheckoutSerializer.CreateSerializerDetails).WithDurableWorkflow(Checkout.Definition);
}

/// <summary>
/// An entity type that has an effect handler registered for its command type and no <c>GaveUp</c> on its definition
/// fails the start of the host: <c>Decide</c> would get nothing when the effect gets no answer.
/// </summary>
public sealed class GiveUpStartupSpecs
{
    private static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> NoGaveUp =
        Counter.Definition with { GaveUp = null };

    private static void ComputeRoute(IServiceCollection services) => services
        .AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>()
        .AddEffectHandler<Compute, CounterCommand, ComputeHandler>();

    [Fact]
    public async Task A_definition_with_no_GaveUp_and_a_handler_that_answers_with_its_command_type_fails_the_start()
    {
        var failure = await StartupProbe.Start(ComputeRoute, builder => StartupProbe.HostCounter(builder, NoGaveUp));

        Assert.NotNull(failure);
        Assert.Contains(
            "Durable entity [counter] cannot start: it issues the effect Compute (handled by ComputeHandler), and its " +
            "definition does not say which command Decide gets when an effect gets no answer. Add the argument " +
            "\"GaveUp: gaveUp => new YourCommand(gaveUp.Reason)\" to the EntityDefinition of [counter], where " +
            "YourCommand is one of its commands (CounterCommand).",
            StartupProbe.Messages(failure));
    }

    [Fact]
    public async Task The_same_host_starts_when_the_definition_has_its_GaveUp()
    {
        var failure = await StartupProbe.Start(ComputeRoute, builder => StartupProbe.HostCounter(builder, Counter.Definition));

        StartupProbe.AssertStarted(failure);
    }

    [Fact]
    public async Task A_definition_with_no_GaveUp_and_no_effect_handler_starts()
    {
        // An entity that issues no effects needs no GaveUp.
        var failure = await StartupProbe.Start(_ => { }, builder => StartupProbe.HostCounter(builder, NoGaveUp));

        StartupProbe.AssertStarted(failure);
    }

    [Fact]
    public async Task A_handler_that_answers_with_the_command_type_of_another_entity_does_not_fail_this_entitys_start()
    {
        // Calculate is the railway's effect. This host runs the counter alone, with no GaveUp.
        var failure = await StartupProbe.Start(
            services => services.AddSingleton<Calculator>().AddSingleton<EffectRecorder>().AddScoped<ScopedCalculator>()
                .AddEffectHandler<Calculate, RailwayCommand, CalculateHandler>(),
            builder => StartupProbe.HostCounter(builder, NoGaveUp));

        StartupProbe.AssertStarted(failure);
    }

    [Fact]
    public async Task Of_two_entity_types_only_the_one_whose_command_type_has_a_handler_needs_its_GaveUp()
    {
        // The handler answers with the railway's command type, and the railway has its GaveUp. The counter has none.
        var failure = await StartupProbe.Start(
            services => services.AddSingleton<Calculator>().AddSingleton<EffectRecorder>().AddScoped<ScopedCalculator>()
                .AddEffectHandler<Calculate, RailwayCommand, CalculateHandler>(),
            builder => StartupProbe.HostCounter(builder, NoGaveUp)
                .WithCustomSerializer(RailwaySerializer.CreateSerializerDetails)
                .WithDurableEntity(Railway.Definition));

        StartupProbe.AssertStarted(failure);

        // The other way round, the start names the railway and not the counter.
        var missing = await StartupProbe.Start(
            services => services.AddSingleton<Calculator>().AddSingleton<EffectRecorder>().AddScoped<ScopedCalculator>()
                .AddEffectHandler<Calculate, RailwayCommand, CalculateHandler>(),
            builder => StartupProbe.HostCounter(builder, NoGaveUp)
                .WithCustomSerializer(RailwaySerializer.CreateSerializerDetails)
                .WithDurableEntity(Railway.Definition with { GaveUp = null }));

        Assert.NotNull(missing);
        Assert.Contains(StartupProbe.Messages(missing), message => message.StartsWith(
            "Durable entity [railway] cannot start: it issues the effect Calculate (handled by CalculateHandler)",
            StringComparison.Ordinal));
    }
}

/// <summary>
/// The serializer check of an entity host covers the effects an entity can issue, found by their handlers'
/// registrations: not a workflow's work, and not in a host that runs no entity type.
/// </summary>
/// <remarks>
/// That an effect with no serializer fails an entity host's start is covered for a handler with an answer by
/// <c>RecordedExecuteStartupSpecs</c>. An effect whose handler has no answer is refused at the first <c>Execute</c>
/// of the entity that issues it, before the write; that is covered by <c>EffectRouteStartupSpecs</c>.
/// </remarks>
public sealed class EffectSerializerStartupSpecs
{
    private static void CounterRoutes(IServiceCollection services) => services
        .AddSingleton<EffectRecorder>().AddSingleton<Gate>().AddScoped<ScopedRecorder>().AddCounterHandlers();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Work_of_a_workflow_with_no_serializer_does_not_fail_an_entity_hosts_start(bool workflowHosted)
    {
        // The checkout's effects are plain records with no serializer. Their handlers answer with the workflow's
        // fact type, so they are not effects of the counter, and a workflow's work is not journaled as an effect.
        var failure = await StartupProbe.Start(
            services =>
            {
                CounterRoutes(services);
                services.AddCheckoutHandlers();
            },
            builder =>
            {
                StartupProbe.HostCounter(builder, Counter.Definition);
                if (workflowHosted) StartupProbe.HostCheckouts(builder);
            });

        StartupProbe.AssertStarted(failure);
    }

    [Fact]
    public async Task A_host_that_runs_only_a_workflow_starts_with_a_no_answer_handler_for_an_effect_with_no_serializer()
    {
        // Only an entity issues an effect whose handler has no answer. No entity type is hosted here.
        var failure = await StartupProbe.Start(
            services => services.AddCheckoutHandlers()
                .AddEffectHandler<EffectRouteStartupSpecs.PlainEffect, EffectRouteStartupSpecs.PlainEffectHandler>(),
            builder => StartupProbe.HostCheckouts(builder));

        StartupProbe.AssertStarted(failure);
    }

    /// <summary>The counter, where <see cref="NoHandler"/> writes one increment and issues a PlainEffect.</summary>
    private static readonly EntityDefinition<CounterState, CounterCommand, CounterEvent> IssuesPlainEffect =
        Counter.Definition with
        {
            Decide = (state, command, ctx) => command.Value is NoHandler
                ? ctx.Persist(new Incremented(1)).Execute(new EffectRouteStartupSpecs.PlainEffect(1))
                : Counter.Decide(state, command, ctx),
        };

    /// <summary>Completes with the text of the first error logged that names PlainEffect.</summary>
    private sealed class PlainEffectErrorSink : ReceiveActor
    {
        public PlainEffectErrorSink(TaskCompletionSource<string> found) =>
            Receive<Error>(e =>
            {
                var text = $"{e.Message} {e.Cause}";
                if (text.Contains("PlainEffect", StringComparison.Ordinal)) found.TrySetResult(text);
            });
    }

    [Fact]
    public async Task The_same_handler_does_not_fail_the_start_next_to_an_entity_type_and_the_entity_that_issues_its_effect_is_refused_before_the_write()
    {
        // A handler with no answer names no entity type, so it fails no entity's start. The counter that issues its
        // effect is refused at that first Execute, before anything is written.
        var ct = TestContext.Current.CancellationToken;
        var patience = TimeSpan.FromSeconds(15);
        CancellationToken Ask()
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(patience);
            return cts.Token;
        }

        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                CounterRoutes(services);
                services.AddCheckoutHandlers()
                    .AddEffectHandler<EffectRouteStartupSpecs.PlainEffect, EffectRouteStartupSpecs.PlainEffectHandler>();
                services.AddAkka("startup-probe", builder =>
                {
                    builder.WithLocalReminders(r => r.WithResolver(system => new DurableEntityReminderResolver(system)));
                    StartupProbe.HostCounter(StartupProbe.HostCheckouts(builder), IssuesPlainEffect);
                });
            })
            .Build();

        Exception? failure = null;
        try
        {
            await host.StartAsync(ct);
        }
        catch (Exception refused)
        {
            failure = refused;
        }

        try
        {
            StartupProbe.AssertStarted(failure);

            var counter = host.Services.GetRequiredService<IDurableEntityRefs<CounterCommand, CounterState, CounterEvent>>()
                .Get(new EntityId("plain-2"));
            await counter.Ask(new Increment(1), Ask()); // the entity runs
            var before = await counter.GetStateAsync(Ask());

            var system = host.Services.GetRequiredService<ActorSystem>();
            var found = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            system.EventStream.Subscribe(system.ActorOf(Props.Create(() => new PlainEffectErrorSink(found))), typeof(Error));

            counter.Tell(new NoHandler()); // one increment and the effect PlainEffect

            var refusal = await found.Task.WaitAsync(patience, ct);
            Assert.Contains("[counter]", refusal);
            Assert.Contains("the effect PlainEffect, handled by PlainEffectHandler", refusal);
            Assert.Contains("no serializer is registered for it", refusal);

            // Refused before the write: the increment of that decision was not stored.
            var after = await counter.GetStateAsync(Ask());
            Assert.Equal(before.SequenceNr, after.SequenceNr);
            Assert.Equal(before.State, after.State);
        }
        finally
        {
            await host.StopAsync(ct);
        }
    }
}

/// <summary>
/// An entity that can issue an effect needs Akka.Reminders, also when the only effect handler has no answer. A host
/// that runs only a workflow is not held to this entity check.
/// </summary>
/// <remarks>The same check for a handler with an answer is covered by <c>OutboxStartupSpecs</c>.</remarks>
public sealed class EffectReminderStartupSpecs
{
    private static void Announcements(IServiceCollection services) => services
        .AddSingleton<EffectRecorder>().AddScoped<ScopedRecorder>()
        .AddEffectHandler<Announcement, AnnounceHandler>();

    [Fact]
    public async Task An_entity_host_with_a_no_answer_handler_fails_the_start_without_Akka_Reminders()
    {
        var failure = await StartupProbe.Start(
            Announcements, builder => StartupProbe.HostCounter(builder, Counter.Definition), reminders: false);

        Assert.NotNull(failure);
        Assert.Contains(StartupProbe.Messages(failure), message => message.StartsWith(
            "Durable entity [counter] cannot start: an entity can issue the effect Announcement (handled by " +
            "AnnounceHandler), and Akka.Reminders is not configured.",
            StringComparison.Ordinal));
    }

    [Fact]
    public async Task The_same_entity_host_starts_when_Akka_Reminders_is_configured()
    {
        var failure = await StartupProbe.Start(
            Announcements, builder => StartupProbe.HostCounter(builder, Counter.Definition), reminders: true);

        StartupProbe.AssertStarted(failure);
    }

    [Fact]
    public async Task An_entity_host_with_no_effect_handler_starts_without_Akka_Reminders()
    {
        var failure = await StartupProbe.Start(
            _ => { }, builder => StartupProbe.HostCounter(builder, Counter.Definition), reminders: false);

        StartupProbe.AssertStarted(failure);
    }

    [Fact]
    public async Task A_host_that_runs_only_a_workflow_is_not_held_to_the_entity_check()
    {
        // The no-answer handler is registered, but no entity type is hosted that could issue its effect.
        var failure = await StartupProbe.Start(
            services =>
            {
                Announcements(services);
                services.AddCheckoutHandlers();
            },
            builder => StartupProbe.HostCheckouts(builder), reminders: false);

        Assert.DoesNotContain(StartupProbe.Messages(failure), message => message.Contains("Announcement"));
        StartupProbe.AssertStarted(failure);
    }
}
