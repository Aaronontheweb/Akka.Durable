using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Durable.Tests.Fixtures.Echoes;
using Akka.Durable.Tests.Fixtures.Nests;
using Akka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Akka.Durable.Tests.Hosting;

/// <summary>Keeps every line written through the host's loggers, with its level.</summary>
internal sealed class LogLines : ILoggerProvider, ILogger
{
    public ConcurrentQueue<(LogLevel Level, string Text)> Lines { get; } = new();
    public ILogger CreateLogger(string categoryName) => this;
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel level, EventId id, TState state, Exception? error, Func<TState, Exception?, string> format) =>
        Lines.Enqueue((level, $"{format(state, error)} {error}"));
    public void Dispose() { }
}

/// <summary>
/// A later deployment whose input, state, fact or outcome type changed reads the journal the nest or echo workflow
/// wrote. It recovers once, stays up, journals nothing and says what is wrong without the stored values.
/// </summary>
public abstract class WorkflowJournalMismatchSpecs(ITestOutputHelper output) : CheckoutWorkflowKit(output)
{
    private const int Level = 48213977; // in the input and the state; it must never reach the log

    private static WorkflowDefinition<TInput, TState, TFact, TOutcome> Reads<TInput, TState, TFact, TOutcome>(
        string name, WorkflowTypeName writer, Func<TInput, TState> start, TFact fact)
        where TInput : notnull where TState : notnull where TFact : notnull where TOutcome : notnull =>
        new(new WorkflowTypeName(name), start, (_, ctx) => ctx.Execute(new Hold()), (_, _, ctx) => ctx.Stay(),
            _ => fact, _ => fact, Nest.ReadsJournalOf(writer));

    protected override void ConfigureTestServices(HostBuilderContext context, IServiceCollection services) =>
        services.AddSingleton<LogLines>()
            .AddSingleton<ILoggerProvider>(provider => provider.GetRequiredService<LogLines>())
            .AddScoped<IEffectHandler<Repeat, EchoFact>, RepeatHandler>()
            .AddScoped<IEffectHandler<Hold, NestFact>, HoldHandler>();

    protected override void ConfigureMore(AkkaConfigurationBuilder builder) => builder
        .WithCustomSerializer(EchoSerializer.CreateSerializerDetails)
        .WithCustomSerializer(NestSerializer.CreateSerializerDetails)
        .WithDurableWorkflow(Nest.Definition, Sharding())
        .WithDurableWorkflow(Echo.Definition, Sharding())
        .WithDurableWorkflow(Reads<Go, Inner, NestFact, Over>("state-changed", Nest.Definition.TypeName,
            go => new Low(go.Level), new Released()), Sharding())
        .WithDurableWorkflow(Reads<Low, Outer, NestFact, Over>("input-changed", Nest.Definition.TypeName,
            low => new Inner(low), new Released()), Sharding())
        .WithDurableWorkflow(Reads<Go, Outer, EchoFact, Over>("fact-changed", Nest.Definition.TypeName,
            go => new Inner(new Low(go.Level)), new EchoLost()), Sharding())
        .WithDurableWorkflow(Reads<Shout, Listening, EchoFact, Echoed>("outcome-changed", Echo.Definition.TypeName,
            _ => new Listening(), new EchoLost()), Sharding());

    private string[] Errors(string about) => Host.Services.GetRequiredService<LogLines>().Lines
        .Where(l => l.Level == LogLevel.Error && l.Text.Contains(about)).Select(l => l.Text).ToArray();

    public static TheoryData<string, string> Commands => new()
    {
        { "state", "start" }, { "state", "notify" }, { "state", "cancel" }, { "state", "wake" }, { "state", "answer" }, { "state", "status" },
        { "input", "start" }, { "input", "notify" }, { "input", "cancel" }, { "input", "wake" }, { "input", "answer" }, { "input", "status" },
    };

    [Theory]
    [MemberData(nameof(Commands))]
    public Task A_workflow_whose_journal_holds_another_type_logs_once_journals_nothing_and_stays_up(string changed, string command) =>
        changed == "state"
            ? Mismatched<Go, Inner>("state-changed", new Go(1), typeof(Outer), typeof(Inner), command, async status =>
            {
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => status);
                Assert.Contains(typeof(Outer).FullName!, ex.Message);
                Assert.Contains(typeof(Inner).FullName!, ex.Message);
            })
            : Mismatched<Low, Outer>("input-changed", new Low(1), typeof(Go), typeof(Low), command, async status =>
                Assert.Equal(Level, Assert.IsType<WorkflowRunning<Outer>>((await status).Value).State switch
                {
                    Inner inner => inner switch { Low low => low.Level, High => -1 },
                    Settled => -2,
                }));

    private async Task Mismatched<TInput, TState>(string type, TInput input, Type stored, Type expected, string command,
        Func<Task<WorkflowStatus<TState, Over>>, Task> check) where TInput : notnull where TState : notnull
    {
        await WorkflowRef<Go, Outer, NestFact, Over>("m1").Start(new Go(Level), AskCancellation());
        var reader = WorkflowRef<TInput, TState, NestFact, Over>("m1");
        var region = Host.Services.GetRequiredService<IRequiredActor<WorkflowRegion<TInput, TState, NestFact, Over>>>().ActorRef;
        var entity = new EntityRef<WfCommand, WfState, WfEvent>(new EntityId("m1"), region);

        // Each recovery logs once, so a restart, by a read or by a command, would show as a second line.
        async Task ReadMany() { for (var i = 0; i < 20; i++) await check(reader.Status(AskCancellation())); }

        await ReadMany();
        var entry = $"[{type}/m1]";
        await AwaitAssertAsync(() => Assert.NotEmpty(Errors(entry)), cancellationToken: TestCancellation);
        var before = (await entity.GetStateAsync(AskCancellation())).SequenceNr;

        // A fact or a cancel gets no reply at all: only a short ask shows it, by running out.
        var shortAsk = AskCancellation(TimeSpan.FromMilliseconds(500));
        switch (command)
        {
            case "start": Assert.IsType<AlreadyRunning>((await reader.Start(input, AskCancellation())).Value); break;
            case "notify": await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.Notify(new Released(), shortAsk)); break;
            case "cancel": await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.Cancel("stop", "ops", shortAsk)); break;
            case "wake": region.Tell(new ShardingEnvelope("m1", (WfCommand)new WfWake(DateTimeOffset.UtcNow))); break;
            case "answer": region.Tell(new ShardingEnvelope("m1", (WfCommand)new WfAnswer(1, 0, (NestFact)new Released()))); break;
        }

        await ReadMany();
        Assert.Equal(before, (await entity.GetStateAsync(AskCancellation())).SequenceNr);
        var line = Assert.Single(Errors(entry));
        Assert.Contains(stored.FullName!, line);
        Assert.Contains(expected.FullName!, line);
        Assert.DoesNotContain(Level.ToString(), line);

        // A healthy workflow of the same type, next to the mismatched one, keeps working.
        var healthy = WorkflowRef<TInput, TState, NestFact, Over>("h1");
        Assert.IsType<Started>((await healthy.Start(input, AskCancellation())).Value);
        Assert.IsType<WorkflowRunning<TState>>((await healthy.Status(AskCancellation())).Value);
    }

    [Fact]
    public async Task A_held_fact_of_another_fact_type_gives_one_error_and_no_restart_loop()
    {
        var writer = WorkflowRef<Go, Outer, NestFact, Over>("m3");
        await writer.Start(new Go(Level), AskCancellation());
        Assert.IsType<Held>((await writer.Notify(new Released(), AskCancellation())).Value);
        var reader = WorkflowRef<Go, Outer, EchoFact, Over>("m3");
        string[] CastErrors() => Errors(typeof(NestFact).FullName!).Where(l => l.Contains(typeof(EchoFact).FullName!)).ToArray();

        // Recovery gives up the interrupted work, then decides the held fact, which is not an EchoFact.
        await reader.Status(AskCancellation());
        await AwaitAssertAsync(() => Assert.NotEmpty(CastErrors()), cancellationToken: TestCancellation);
        for (var i = 0; i < 20; i++) Assert.IsType<WorkflowRunning<Outer>>((await reader.Status(AskCancellation())).Value);

        Assert.Single(CastErrors());
    }

    [Fact]
    public async Task A_journal_with_another_outcome_type_fails_the_status_read_and_names_both_types()
    {
        var writer = WorkflowRef<Shout, Listening, EchoFact, Heard>("m4");
        await writer.Start(new Shout("hi"), AskCancellation());
        await AwaitAssertAsync(async () => Assert.IsType<WorkflowFinished<Heard>>((await writer.Status(AskCancellation())).Value),
            cancellationToken: TestCancellation);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            WorkflowRef<Shout, Listening, EchoFact, Echoed>("m4").Status(AskCancellation()));

        Assert.Contains(typeof(Heard).FullName!, ex.Message);
        Assert.Contains(typeof(Echoed).FullName!, ex.Message);
    }
}

public sealed class LocalWorkflowJournalMismatchSpecs(ITestOutputHelper output) : WorkflowJournalMismatchSpecs(output);

public sealed class ShardedWorkflowJournalMismatchSpecs(ITestOutputHelper output) : WorkflowJournalMismatchSpecs(output)
{
    protected override bool Sharded => true;
}
