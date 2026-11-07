using Akka.Durable.Abstractions.Entities;
using Akka.Durable.Entities;
using Akka.Durable.Entities.Hosting;
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
/// <item><see cref="IEntityRefs{TCommand}"/> and <see cref="IDurableEntityRefs{TCommand,TState}"/> registered
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

    /// <summary>Register durable entities (and any other actors) for the test host.</summary>
    protected abstract void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider);

    /// <summary>Register application services, e.g. those resolved by <c>Run</c> effects.</summary>
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
    protected IDurableEntityRefs<TCommand, TState> DurableEntityRefs<TCommand, TState>() =>
        Host.Services.GetRequiredService<IDurableEntityRefs<TCommand, TState>>();

    /// <summary>A reference to one entity that can also read its state.</summary>
    protected IDurableEntityRef<TCommand, TState> DurableEntityRef<TCommand, TState>(string entityId) =>
        DurableEntityRefs<TCommand, TState>().Get(new EntityId(entityId));

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
