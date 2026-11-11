namespace Akka.Durable;

/// <summary>
/// Marks every record that Akka.Durable itself writes to a journal or a snapshot store: the records of an entity's
/// <c>Execute</c>, a workflow's events, and the envelope of an entity's snapshot.
/// </summary>
/// <remarks>
/// <para>A reader of the journal, such as a projection or a migration tool, finds the library's records with one type
/// test: <c>if (entry is IDurableJournalRecord) continue;</c>. Your own events never carry it.</para>
/// <para>The types that carry it are internal. Their contents can change between versions; do not read them.</para>
/// </remarks>
public interface IDurableJournalRecord;
