using System.Collections.Immutable;
using Akka.Durable.Tests.Fixtures;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace Akka.Durable.Tests;

/// <summary>Compile real callers to prove invalid calls cannot escape through overload resolution.</summary>
public sealed class TypedContractSpecs
{
    private static readonly ImmutableArray<MetadataReference> References =
        ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator)
        .Append(typeof(IEntityRef<>).Assembly.Location)
        .Append(typeof(Counter).Assembly.Location)
        .Distinct(StringComparer.Ordinal)
        .Select(path => MetadataReference.CreateFromFile(path))
        .ToImmutableArray<MetadataReference>();

    private static ImmutableArray<Diagnostic> Compile(string body)
    {
        var source = """
            using System;
            using System.Threading;
            using System.Threading.Tasks;
            using Akka.Actor;
            using Akka.Durable;
            using Akka.Durable.Tests.Fixtures;
            public static class Caller
            {
                public static void Call(IEntityRef<CounterCommand> entity,
                    DecisionContext<CounterState, CounterCommand, CounterEvent> ctx)
                {
            """ + body + """
                }
            }
            """;
        var syntax = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Preview));
        return CSharpCompilation.Create("TypedContractProbe", [syntax], References,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary))
            .GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error)
            .ToImmutableArray();
    }

    [Fact]
    public void Command_contract_infers_Ask_and_bound_reply_types()
    {
        var errors = Compile("""
            Task<Accepted> accepted = entity.Ask(new Increment(1));
            Task<CountIs> count = entity.Ask(new GetCount());
            var request = ctx.For(new Increment(1));
            Decision<CounterCommand, CounterEvent> decision = request.Reply(new Accepted());
            var graph = request.Run<Gate>(
                (gate, ct) => gate.WaitAsync(ct),
                onSuccess: () => request.Reply(new Accepted()),
                onFailure: error => request.Reply(new Accepted()));
            var resultGraph = request.Run<Gate, int>(
                async (gate, ct) => { await gate.WaitAsync(ct); return 1; },
                onSuccess: (result, next) => next.Reply(new Accepted()),
                onFailure: (error, next) => next.Reply(new Accepted()));
            """);

        Assert.True(errors.IsEmpty, string.Join(Environment.NewLine, errors));
    }

    public static TheoryData<string> InvalidCalls =>
    [
        "entity.Ask(new Noop());",
        "entity.Ask<CountIs>(new Increment(1));",
        "entity.Ask(new Adjust(1));",
        "ICanTell untyped = entity;",
        "ctx.For(new Increment(1)).Reply(new CountIs(0));",
        "ctx.None().Reply(new Accepted());",
        "ctx.For(new Increment(1)).Reply(new Accepted()).Reply(new Accepted());",
        """
        var request = ctx.For(new Increment(1));
        request.Run<Gate>((gate, ct) => gate.WaitAsync(ct),
            onSuccess: () => request.Reply(new Accepted()));
        """,
        """
        var request = ctx.For(new Increment(1));
        request.Run<Gate>((gate, ct) => gate.WaitAsync(ct),
            onFailure: error => request.Reply(new Accepted()));
        """,
        """
        var request = ctx.For(new Increment(1));
        request.Run<Gate, int>((gate, ct) => Task.FromResult(1),
            onSuccess: result => request.Reply(new Accepted()));
        """,
        """
        var request = ctx.For(new Increment(1));
        request.Run<Gate>((gate, ct) => gate.WaitAsync(ct),
            onSuccess: () => request.Reply(new Accepted()),
            onFailure: error => request.Reply(new Accepted())).Reply(new Accepted());
        """,
        """
        var request = ctx.For(new Increment(1));
        request.Run<Gate>((gate, ct) => gate.WaitAsync(ct),
            onSuccess: () => ctx.For(new GetCount()).Reply(new CountIs(0)),
            onFailure: error => request.Reply(new Accepted()));
        """,
        """
        var request = ctx.For(new Increment(1));
        request.Run<Gate, int>((gate, ct) => Task.FromResult(1),
            onSuccess: result => request.Reply(new Accepted()),
            onFailure: error => ctx.For(new GetCount()).Reply(new CountIs(0)));
        """
    ];

    [Theory]
    [MemberData(nameof(InvalidCalls))]
    public void Invalid_calls_fail_compilation(string body) => Assert.NotEmpty(Compile(body));
}
