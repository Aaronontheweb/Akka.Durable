using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Durable.TestKit;
using Akka.Hosting;
using Akka.Persistence.Sql.Hosting;
using Akka.Reminders;
using Akka.Serialization;
using Akka.Serialization.V2;
using LinqToDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

// Its own namespace. The command case below has the name the guide gives it, EffectGaveUp. The library has an internal
// record of that name, which this test assembly can see and an application cannot. Inside this namespace the name
// means the command case.
namespace Akka.Durable.Tests.Hosting.Invoices;

using InvoiceContext = DecisionContext<InvoiceState, InvoiceCommand, InvoiceEvent>;
using InvoiceDecision = Decision<InvoiceCommand, InvoiceEvent>;

// The sample of the guide's section "One GaveUp for all the effects", in an entity whose types all go through one
// source-generated serializer: one command case carries the effect that got no answer, and Decide matches on it.

public interface IInvoiceProtocol;

// States
[AkkaSerializable(Manifest = "invoice-draft-v1", AllowEmpty = true)]
public sealed record Draft : IInvoiceProtocol;

/// <param name="Recipient">Who gets the reminders.</param>
/// <param name="Postponed">How many reminders got no answer and were put off.</param>
/// <param name="LastGaveUp">Why the last one got no answer. Empty when none did.</param>
[AkkaSerializable(Manifest = "invoice-issued-v1")]
public sealed record Issued(
    [property: AkkaField(0)] string Recipient,
    [property: AkkaField(1)] int Postponed,
    [property: AkkaField(2)] string LastGaveUp) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-state-v1")]
public readonly union InvoiceState(Draft, Issued) : IInvoiceProtocol;

// Commands
[AkkaSerializable(Manifest = "invoice-issue-v1")]
public sealed record IssueInvoice([property: AkkaField(0)] string Recipient)
    : IInvoiceProtocol, IReplies<InvoiceCommand, InvoiceAccepted>
{
    public InvoiceCommand ToCommand() => new(this);
}

/// <summary>Issues a <see cref="MailReminder"/>.</summary>
[AkkaSerializable(Manifest = "invoice-send-reminder-v1", AllowEmpty = true)]
public sealed record SendReminder : IInvoiceProtocol;

/// <summary>The answer of <see cref="MailReminder"/>.</summary>
[AkkaSerializable(Manifest = "invoice-reminder-delivered-v1", AllowEmpty = true)]
public sealed record ReminderDelivered : IInvoiceProtocol;

/// <summary>Issues a <see cref="PublishIssued"/>.</summary>
[AkkaSerializable(Manifest = "invoice-publish-v1", AllowEmpty = true)]
public sealed record PublishInvoice : IInvoiceProtocol;

// A command case. Work holds the effect that got no answer.
[AkkaSerializable(Manifest = "invoice-effect-gave-up-v1")]
public sealed record EffectGaveUp(
    [property: AkkaField(0)] object Work,
    [property: AkkaField(1)] GaveUpReason Reason) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-command-v1")]
public readonly union InvoiceCommand(IssueInvoice, SendReminder, ReminderDelivered, PublishInvoice, EffectGaveUp)
    : IInvoiceProtocol;

// Events
[AkkaSerializable(Manifest = "invoice-issued-event-v1")]
public sealed record InvoiceIssued([property: AkkaField(0)] string Recipient) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-reminder-postponed-v1")]
public sealed record ReminderPostponed([property: AkkaField(0)] GaveUpReason Reason) : IInvoiceProtocol;

[AkkaSerializable(Manifest = "invoice-event-v1")]
public readonly union InvoiceEvent(InvoiceIssued, ReminderPostponed) : IInvoiceProtocol;

// Reply
[AkkaSerializable(Manifest = "invoice-accepted-v1", AllowEmpty = true)]
public sealed record InvoiceAccepted : IInvoiceProtocol;

// Effects. Plain records, journaled with the decision that issues them.

/// <summary>The effect Decide has an arm of its own for.</summary>
[AkkaSerializable(Manifest = "invoice-mail-reminder-v1")]
public sealed record MailReminder([property: AkkaField(0)] string Recipient) : IInvoiceProtocol;

/// <summary>An effect whose give-up falls to the blanket arm. Its handler has no answer.</summary>
[AkkaSerializable(Manifest = "invoice-publish-issued-v1")]
public sealed record PublishIssued([property: AkkaField(0)] string Invoice) : IInvoiceProtocol;

[AkkaSerializer<IInvoiceProtocol>("invoice-test", 22610)]
public sealed partial class InvoiceSerializer : AkkaSerializer
{
    public static partial SerializerDetails CreateSerializerDetails(ExtendedActorSystem system);
}

/// <summary>What the handlers were given and do, and every command Decide received.</summary>
public sealed class InvoiceWorld
{
    /// <summary>The effect of every handler call, in order.</summary>
    public ConcurrentQueue<object> Calls { get; } = new();

    /// <summary>Every command Decide received, in order.</summary>
    public ConcurrentQueue<object> Decided { get; } = new();

    /// <summary>Runs in each handler call after it was recorded. Throw to fail the attempt. Default: returns at once.</summary>
    public Func<object, CancellationToken, Task> OnCall { get; set; } = (_, _) => Task.CompletedTask;

    public EffectGaveUp[] GiveUps => [.. Decided.OfType<EffectGaveUp>()];
}

public sealed class MailReminderHandler(InvoiceWorld world) : IEffectHandler<MailReminder, InvoiceCommand>
{
    public async Task<InvoiceCommand> HandleAsync(MailReminder effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        world.Calls.Enqueue(effect);
        await world.OnCall(effect, cancellationToken);
        return new ReminderDelivered();
    }
}

public sealed class PublishIssuedHandler(InvoiceWorld world) : IEffectHandler<PublishIssued>
{
    public async Task HandleAsync(PublishIssued effect, EffectDelivery delivery, CancellationToken cancellationToken)
    {
        world.Calls.Enqueue(effect);
        await world.OnCall(effect, cancellationToken);
    }
}

public static class Invoice
{
    public static readonly EntityTypeName TypeName = new("invoices");

    public static IServiceCollection AddInvoice(this IServiceCollection services, InvoiceWorld world) => services
        .AddSingleton(world)
        .AddEffectHandler<MailReminder, InvoiceCommand, MailReminderHandler>()
        .AddEffectHandler<PublishIssued, PublishIssuedHandler>();

    /// <summary><paramref name="world"/> only records the commands Decide receives.</summary>
    public static EntityDefinition<InvoiceState, InvoiceCommand, InvoiceEvent> Definition(InvoiceWorld world) => new(
        TypeName,
        _ => new Draft(),
        (state, command, ctx) =>
        {
            world.Decided.Enqueue(command.Value!);
            return Decide(state, command, ctx);
        },
        Evolve,
        // On the invoice's definition.
        GaveUp: g => new EffectGaveUp(g.Work, g.Reason));

    // The state comes first. Each state lists only its own commands, then one blanket arm.
    public static InvoiceDecision Decide(InvoiceState state, InvoiceCommand command, InvoiceContext ctx) =>
        state switch
        {
            Draft => command switch
            {
                IssueInvoice c => ctx.Persist(new InvoiceIssued(c.Recipient)).Reply(c, new InvoiceAccepted()),
                _ => NotHere(command, ctx),
            },
            Issued s => command switch
            {
                SendReminder => ctx.None().Execute(new MailReminder(s.Recipient)),
                PublishInvoice => ctx.None().Execute(new PublishIssued(ctx.EntityId.Value)),

                // The one effect this state cares about. The guide's arm, and one event so that a spec sees it in state.
                EffectGaveUp { Work: MailReminder } g =>
                    ctx.Persist(new ReminderPostponed(g.Reason))
                        .Schedule("reminder", TimeSpan.FromHours(1), new SendReminder()),
                _ => NotHere(command, ctx),
            },
        };

    // The blanket arm of every state.
    private static InvoiceDecision NotHere(InvoiceCommand command, InvoiceContext ctx) =>
        command switch
        {
            IssueInvoice or SendReminder or ReminderDelivered or PublishInvoice => ctx.Ignore(),

            // Every other give-up.
            EffectGaveUp => ctx.Ignore(),
        };

    public static InvoiceState Evolve(InvoiceState state, InvoiceEvent @event) =>
        (state, @event) switch
        {
            (_, InvoiceIssued issued) => new Issued(issued.Recipient, Postponed: 0, LastGaveUp: ""),
            (Issued s, ReminderPostponed postponed) =>
                s with { Postponed = s.Postponed + 1, LastGaveUp = postponed.Reason.ToString() },
            _ => state,
        };
}

/// <summary>
/// The command case that carries the effect, through the actor system's serializer: the effect in <c>Work</c> is
/// written by the serializer of its own type and comes back equal by value.
/// </summary>
public sealed class GiveUpCommandSerializationSpecs : IAsyncLifetime
{
    private readonly ActorSystem _system = ActorSystem.Create("give-up-command",
        BootstrapSetup.Create().And(SerializationSetup.Create(system =>
            [InvoiceSerializer.CreateSerializerDetails(system)])));

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync() => await _system.Terminate();

    public static TheoryData<GaveUpReason> Reasons =>
        [GaveUpReason.Failed, GaveUpReason.TimedOut, GaveUpReason.Interrupted, GaveUpReason.RetriesExhausted];

    private EffectGaveUp RoundTrip(EffectGaveUp gaveUp)
    {
        // As the entity gets it: a case of its command union.
        object command = (InvoiceCommand)gaveUp;
        var serializer = _system.Serialization.FindSerializerFor(command);
        Assert.Equal(22610, serializer.Identifier); // the invoice's own serializer, not a fallback
        var bytes = serializer.ToBinary(command);
        var manifest = Akka.Serialization.Serialization.ManifestFor(serializer, command);

        var back = Assert.IsType<InvoiceCommand>(_system.Serialization.Deserialize(bytes, serializer.Identifier, manifest));
        return Assert.IsType<EffectGaveUp>(back.Value);
    }

    [Theory]
    [MemberData(nameof(Reasons))]
    public void The_command_round_trips_inside_its_union_with_the_effect_equal_by_value_and_the_reason(GaveUpReason reason)
    {
        var mail = RoundTrip(new EffectGaveUp(new MailReminder("billing@example.com"), reason));
        Assert.Equal(new MailReminder("billing@example.com"), Assert.IsType<MailReminder>(mail.Work));
        Assert.Equal(reason, mail.Reason);

        var publish = RoundTrip(new EffectGaveUp(new PublishIssued("i-7"), reason));
        Assert.Equal(new PublishIssued("i-7"), Assert.IsType<PublishIssued>(publish.Work));
        Assert.Equal(reason, publish.Reason);
    }

    [Fact]
    public void The_arm_for_one_effect_type_matches_the_command_that_was_read_back()
    {
        var at = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var ctx = new InvoiceContext(new EntityId("i-7"), Invoice.TypeName, at);
        var issued = new Issued("billing@example.com", Postponed: 0, LastGaveUp: "");

        var own = Invoice.Decide(issued, RoundTrip(new EffectGaveUp(new MailReminder("billing@example.com"), GaveUpReason.Failed)), ctx);
        var blanket = Invoice.Decide(issued, RoundTrip(new EffectGaveUp(new PublishIssued("i-7"), GaveUpReason.Failed)), ctx);

        var handled = Assert.IsType<Handled<InvoiceCommand, InvoiceEvent>>(own.Value);
        Assert.Equal<object?>([new ReminderPostponed(GaveUpReason.Failed)], handled.Events.Select(e => e.Value));
        Assert.IsType<Ignored>(blanket.Value);
    }
}

/// <summary>The two arms through <c>DeciderScenario.WhenGaveUp</c>. No ActorSystem.</summary>
public sealed class GiveUpCommandDeciderSpecs
{
    private static DeciderScenario<InvoiceState, InvoiceCommand, InvoiceEvent> Issued() =>
        DeciderScenario.For(Invoice.Definition(new InvoiceWorld())).Given(new InvoiceIssued("billing@example.com"));

    [Fact]
    public void The_give_up_of_the_effect_the_state_cares_about_takes_its_own_arm()
    {
        var outcome = Issued().WhenGaveUp(new MailReminder("billing@example.com"), GaveUpReason.TimedOut);

        outcome
            .ThenPersisted(new ReminderPostponed(GaveUpReason.TimedOut))
            .ThenState(state => Assert.Equal(new Issued("billing@example.com", 1, "TimedOut"), state.Value))
            .ThenEffects(typeof(ScheduleEffect));
        var schedule = Assert.IsType<ScheduleEffect>(outcome.Effects.Single().Value);
        Assert.Equal("reminder", schedule.Key);
        Assert.Equal(TimeSpan.FromHours(1), schedule.Delay);
        Assert.True(schedule.Message is InvoiceCommand { Value: SendReminder });
    }

    [Fact]
    public void The_give_up_of_every_other_effect_is_ignored_by_the_blanket_arm()
    {
        Issued().WhenGaveUp(new PublishIssued("test-entity"), GaveUpReason.RetriesExhausted, attempts: 3).ThenIgnored();

        // The arm belongs to its state: in a draft, the same give-up falls to the blanket arm too.
        DeciderScenario.For(Invoice.Definition(new InvoiceWorld()))
            .WhenGaveUp(new MailReminder("billing@example.com"), GaveUpReason.Failed)
            .ThenIgnored();
    }
}

/// <summary>
/// The invoice hosted through the public API, with every message serialized and read back, the journal's included.
/// </summary>
public sealed class GiveUpCommandWithEffectSpecs(ITestOutputHelper output) : DurableEntityTestKit(output)
{
    private const string Recipient = "billing@example.com";

    private readonly InvoiceWorld _world = new();

    protected override bool VerifyMessageSerialization => true;

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddInvoice(_world);

    protected override void ConfigureEntities(AkkaConfigurationBuilder builder, IServiceProvider provider) =>
        builder.WithCustomSerializer(InvoiceSerializer.CreateSerializerDetails)
            .WithDurableEntity(Invoice.Definition(_world));

    private IDurableEntityRef<InvoiceCommand, InvoiceState, InvoiceEvent> InvoiceRef(string id) =>
        DurableEntityRef<InvoiceCommand, InvoiceState, InvoiceEvent>(id);

    private async Task<StateSnapshot<InvoiceState>> Read(string id) => await InvoiceRef(id).GetStateAsync(AskCancellation());

    private Task AwaitNoPending(string id) => AwaitAssertAsync(
        async () => Assert.Empty(await PendingEffects<InvoiceCommand>(id)), cancellationToken: TestCancellation);

    private async Task<IReadOnlyList<ScheduledReminder>> RemindersOf(string id)
    {
        var listed = await Sys.ReminderClient().CreateClient(Invoice.TypeName.Value, id).ListRemindersAsync(TestCancellation);
        Assert.Equal(FetchRemindersResponseCode.Success, listed.ResponseCode);
        return listed.Reminders;
    }

    /// <summary>Kills the entity's actor and waits until its host has started a new one, which has to recover.</summary>
    private async Task<StateSnapshot<InvoiceState>> Restart(string id)
    {
        var path = $"/user/{LocalEntityHost.ActorName(Invoice.TypeName)}/{Uri.EscapeDataString(id)}";
        var actor = await Sys.ActorSelection(path).ResolveOne(TimeSpan.FromSeconds(5), TestCancellation);
        Watch(actor);
        actor.Tell(PoisonPill.Instance);
        await ExpectTerminatedAsync(actor, cancellationToken: TestCancellation);

        // The host learns that the actor stopped a moment after this spec does, so the first read can be lost.
        StateSnapshot<InvoiceState>? recovered = null;
        await AwaitAssertAsync(
            async () => recovered = await InvoiceRef(id).GetStateAsync(AskCancellation(TimeSpan.FromMilliseconds(500))),
            TimeSpan.FromSeconds(15), cancellationToken: TestCancellation);
        return recovered!;
    }

    [Fact]
    public async Task An_effect_whose_handler_throws_gives_up_into_the_arm_for_its_effect_type()
    {
        _world.OnCall = (_, _) => throw new InvalidOperationException("the mail server is down");
        await InvoiceRef("i1").Ask(new IssueInvoice(Recipient), AskCancellation());

        InvoiceRef("i1").Tell(new SendReminder());

        await AwaitAssertAsync(
            async () => Assert.Equal(new Issued(Recipient, Postponed: 1, LastGaveUp: "Failed"), (await Read("i1")).State.Value),
            cancellationToken: TestCancellation);
        await AwaitNoPending("i1");

        // Decide got the command the definition's GaveUp made, with the effect by value, once.
        Assert.Equal([new EffectGaveUp(new MailReminder(Recipient), GaveUpReason.Failed)], _world.GiveUps);
        Assert.Equal<object>([new MailReminder(Recipient)], _world.Calls);

        // The arm's Schedule: the reminder that asks again in an hour is stored.
        await AwaitAssertAsync(
            async () => Assert.Contains(await RemindersOf("i1"), reminder => reminder.Key == new ReminderKey("reminder")),
            cancellationToken: TestCancellation);

        // Settled: a new incarnation of the entity gives nothing up again.
        var recovered = await Restart("i1");
        Assert.Equal(new Issued(Recipient, Postponed: 1, LastGaveUp: "Failed"), recovered.State.Value);
        Assert.Single(_world.GiveUps);
    }

    [Fact]
    public async Task The_give_up_of_another_effect_type_is_ignored_by_the_blanket_arm_and_the_effect_is_settled()
    {
        _world.OnCall = (_, _) => throw new InvalidOperationException("the bus is down");
        await InvoiceRef("i2").Ask(new IssueInvoice(Recipient), AskCancellation());
        var before = await Read("i2");

        InvoiceRef("i2").Tell(new PublishInvoice());

        await AwaitAssertAsync(() => Assert.Single(_world.GiveUps), cancellationToken: TestCancellation);
        await AwaitNoPending("i2");
        Assert.Equal([new EffectGaveUp(new PublishIssued("i2"), GaveUpReason.Failed)], _world.GiveUps);
        Assert.Equal<object>([new PublishIssued("i2")], _world.Calls);

        // Decide ignored it: the state did not change. The effect and its give-up were journaled all the same.
        var after = await Read("i2");
        Assert.Equal(new Issued(Recipient, Postponed: 0, LastGaveUp: ""), after.State.Value);
        Assert.Equal(before.State.Value, after.State.Value);
        Assert.Equal(before.SequenceNr + 2, after.SequenceNr);

        // So a new incarnation finds the effect settled: it is not given up again, and nothing more is written.
        var recovered = await Restart("i2");
        await AwaitNoPending("i2");
        Assert.Equal(after.State.Value, recovered.State.Value);
        Assert.Equal(after.SequenceNr, (await Read("i2")).SequenceNr);
        Assert.Single(_world.GiveUps);
        Assert.Single(_world.Calls);
    }
}

/// <summary>
/// The effect read back from stored bytes (Akka.Persistence.Sql on SQLite) by a new ActorSystem: it is given up as
/// interrupted, and the arm for its effect type matches the effect the journal gave back.
/// </summary>
public sealed class GiveUpCommandWithEffectSqlSpecs : IDisposable
{
    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"akka-durable-{Guid.NewGuid():N}.db");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static CancellationToken AskTimeout()
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        return cts.Token;
    }

    private async Task<IHost> StartHost(InvoiceWorld world)
    {
        var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddDurableEntities();
                services.AddInvoice(world);
                services.AddAkka("give-up-command-sql", builder => builder
                    .WithSqlPersistence(connectionString: $"Data Source={_databasePath}", providerName: ProviderName.SQLiteMS)
                    .WithLocalReminders(reminders => reminders.WithResolver(system => new DurableEntityReminderResolver(system)))
                    .WithCustomSerializer(InvoiceSerializer.CreateSerializerDetails)
                    .WithDurableEntity(Invoice.Definition(world)));
            })
            .Build();

        await host.StartAsync(Ct);
        return host;
    }

    private static IDurableEntityRef<InvoiceCommand, InvoiceState, InvoiceEvent> InvoiceRef(IHost host) =>
        host.Services.GetRequiredService<IDurableEntityRefs<InvoiceCommand, InvoiceState, InvoiceEvent>>()
            .Get(new EntityId("i3"));

    [Fact]
    public async Task An_effect_pending_in_stored_bytes_gives_up_into_the_arm_for_its_effect_type()
    {
        var first = new InvoiceWorld { OnCall = (_, ct) => Task.Delay(Timeout.Infinite, ct) }; // the mail never answers
        using (var writer = await StartHost(first))
        {
            await InvoiceRef(writer).Ask(new IssueInvoice("billing@example.com"), AskTimeout());
            InvoiceRef(writer).Tell(new SendReminder());

            // The handler runs only after the write.
            while (first.Calls.IsEmpty) await Task.Delay(20, Ct);
            await writer.StopAsync(Ct);
        }

        var second = new InvoiceWorld();
        using var reader = await StartHost(second);
        var state = (await InvoiceRef(reader).GetStateAsync(AskTimeout())).State;

        Assert.Equal(new Issued("billing@example.com", Postponed: 1, LastGaveUp: "Interrupted"), state.Value);
        Assert.Equal([new EffectGaveUp(new MailReminder("billing@example.com"), GaveUpReason.Interrupted)], second.GiveUps);
        Assert.Empty(second.Calls); // not run again
        await reader.StopAsync(Ct);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (File.Exists(_databasePath)) File.Delete(_databasePath);
    }
}
