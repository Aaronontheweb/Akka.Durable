using Akka.Hosting;
using Akka.Reminders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Akka.Durable.TestKit;

/// <summary>
/// Base class for hosted durable entity tests.
/// </summary>
/// <remarks>
/// <para>Each test gets a fresh host with:</para>
/// <list type="bullet">
/// <item>the Akka.Persistence TestKit journal and snapshot store (with fault injection via
/// <c>WithJournalWrite</c>, <c>WithSnapshotSave</c>, ...);</item>
/// <item>Akka.Reminders running locally with in-memory storage, delivering to durable entities in either
/// hosting mode;</item>
/// <item><see cref="IEntityRefs{TCommand}"/> and <see cref="IDurableEntityRefs{TCommand,TState,TEvent}"/> registered
/// for injection.</item>
/// </list>
/// <para>Register entities in <see cref="ConfigureEntities"/>, typically with
/// <c>WithDurableEntity(definition)</c>.</para>
/// </remarks>
public abstract class DurableEntityTestKit(
    ITestOutputHelper? output = null,
    string? actorSystemName = null,
    TimeSpan? startupTimeout = null,
    LogLevel logLevel = LogLevel.Information)
    : Akka.Hosting.TestKit.PersistenceTestKit(actorSystemName, output, startupTimeout, logLevel)
{
    /// <summary>Test-friendly reminder settings: short slippage and fast retries.</summary>
    protected virtual ReminderSettings ReminderSettings { get; } = new()
    {
        MaxSlippage = TimeSpan.FromMilliseconds(500),
        MaxDeliveryAttempts = 3,
        RetryBackoffBase = TimeSpan.FromMilliseconds(100),
    };

    /// <summary>
    /// Run with <c>akka.actor.serialize-messages = on</c>, so every message is serialized and deserialized as
    /// if it crossed the network. Off by default.
    /// </summary>
    /// <remarks>
    /// Akka.NET 1.6.0-beta6 verifies the journal's internal reply wrapper with the JSON fallback serializer, which
    /// empties C# union events on every successful persist (akkadotnet/akka.net#8910). Enable this only for
    /// entities whose events survive JSON, until that is fixed.
    /// </remarks>
    protected virtual bool VerifyMessageSerialization => false;

    /// <summary>Register durable entities (and any other actors) for the test host.</summary>
    protected abstract void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider);

    /// <summary>Register application services: effect handlers and the services <c>Run</c> effects resolve.</summary>
    protected virtual void ConfigureTestServices(HostBuilderContext context, IServiceCollection services)
    {
    }

    protected sealed override void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        base.ConfigureServices(context, services);
        services.AddDurableEntities();
        ConfigureTestServices(context, services);
    }

    protected sealed override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        base.ConfigureAkka(builder, provider);

        if (VerifyMessageSerialization)
            builder.AddHocon("akka.actor.serialize-messages = on", HoconAddMode.Prepend);

        builder.WithLocalReminders(reminders => reminders
            .WithInMemoryStorage()
            .WithResolver(system => new DurableEntityReminderResolver(system))
            .WithSettings(ReminderSettings));

        ConfigureEntities(builder, provider);
    }

    /// <summary>The typed reference factory for an entity type, as application code would inject it.</summary>
    protected IEntityRefs<TCommand> EntityRefs<TCommand>() =>
        Host.Services.GetRequiredService<IEntityRefs<TCommand>>();

    /// <summary>A logical reference to one entity.</summary>
    protected IEntityRef<TCommand> EntityRef<TCommand>(string entityId) =>
        EntityRefs<TCommand>().Get(new EntityId(entityId));

    /// <summary>The state-aware reference factory for an entity type, as application code would inject it.</summary>
    protected IDurableEntityRefs<TCommand, TState, TEvent> DurableEntityRefs<TCommand, TState, TEvent>() =>
        Host.Services.GetRequiredService<IDurableEntityRefs<TCommand, TState, TEvent>>();

    /// <summary>A reference to one entity that can also read its state.</summary>
    protected IDurableEntityRef<TCommand, TState, TEvent> DurableEntityRef<TCommand, TState, TEvent>(string entityId) =>
        DurableEntityRefs<TCommand, TState, TEvent>().Get(new EntityId(entityId));

    /// <summary>A logical reference to one workflow, as application code would get it from <see cref="IWorkflowRefs{TInput,TOutcome}"/>.</summary>
    protected IWorkflowRef<TInput, TOutcome> WorkflowRef<TInput, TOutcome>(string workflowId) =>
        Host.Services.GetRequiredService<IWorkflowRefs<TInput, TOutcome>>().Get(new WorkflowId(workflowId));

    /// <summary>The current test's cancellation token, for awaited calls.</summary>
    protected static CancellationToken TestCancellation => TestContext.Current.CancellationToken;

    /// <summary>Default bound for <see cref="AskCancellation"/>.</summary>
    protected virtual TimeSpan DefaultAskTimeout => TimeSpan.FromSeconds(15);

    /// <summary>
    /// A token for one <see cref="IEntityRef{TCommand}.Ask{TReply}"/> that cancels after
    /// <paramref name="timeout"/> (default <see cref="DefaultAskTimeout"/>) or when the test is cancelled,
    /// so a lost message fails the test instead of hanging the run.
    /// </summary>
    protected CancellationToken AskCancellation(TimeSpan? timeout = null)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(TestCancellation);
        cts.CancelAfter(timeout ?? DefaultAskTimeout);
        return cts.Token;
    }
}
