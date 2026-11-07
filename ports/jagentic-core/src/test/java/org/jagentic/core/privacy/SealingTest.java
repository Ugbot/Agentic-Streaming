package org.jagentic.core.privacy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.jagentic.core.ConversationLog;
import org.jagentic.core.ConversationStore;
import org.jagentic.core.EffectJournal;
import org.jagentic.core.Event;
import org.jagentic.core.KeyedStateStore;
import org.jagentic.core.LocalRuntime;
import org.jagentic.core.LogEvent;
import org.jagentic.core.Principal;
import org.jagentic.core.TurnResult;
import org.jagentic.core.TurnStatus;
import org.jagentic.core.pipeline.GraphBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A deletion request on a log nobody can rewrite: destroy the key, and what was sealed is gone. */
class SealingTest {

  private static final String TEXT = "my card is under the mat, look it up";

  @TempDir Path dir;
  private FileKeyVault vault;
  private ConversationLog raw;
  private EffectJournal.InMemory journal;
  private LocalRuntime runtime;

  @BeforeEach
  void start() {
    vault = new FileKeyVault(dir);
    raw = new ConversationLog.InMemory();
    journal = new EffectJournal.InMemory();
    GraphBuilder.Built built = GraphBuilder.build(Map.of(
        "spec_version", "agentic/v1",
        "agent", Map.of("id", "a", "router", Map.of("kind", "static", "default", "main"),
            "paths", Map.of("main", Map.of("brain", "rule", "tools", List.of("find"), "tool_triggers", Map.of("look", "find"))),
            "verifier", Map.of("kind", "none")),
        "tools", List.of(Map.of("id", "find", "kind", "constant", "effect", "read", "value", "under the mat"))), null);
    runtime = new LocalRuntime(built.graph(), new ConversationStore.InMemory(), new KeyedStateStore.InMemory(),
        built.tools(), null, new SealedLog(raw, vault)).withJournal(new SealedJournal(journal, vault));
  }

  private TurnResult turn(String cid, String id, Principal who) {
    return runtime.submit(Event.turn(cid, id, "", TEXT).withPrincipal(who));
  }

  private static String dump(List<LogEvent> events) {
    return events.stream().map(e -> e.payload().toString()).reduce("", String::concat);
  }

  @Test
  void whatIsStoredIsSealedAndWhatIsReadIsNot() {
    TurnResult r = turn("c1", "t1", Principal.of("alice", "acme"));
    assertEquals(TurnStatus.COMPLETED, r.status);
    assertTrue(r.reply.contains("under the mat"));
    String stored = dump(raw.events("c1"));
    assertFalse(stored.contains("under the mat"), stored);
    assertFalse(stored.contains("my card"), stored);
    assertTrue(dump(runtime.log().events("c1")).contains(TEXT));
    assertFalse(journal.get(new EffectJournal.Key("c1", "t1", EffectJournal.TOOL, 0)).orElseThrow().result()
        .toString().contains("under the mat"));
  }

  @Test
  void erasingAnOwnerLeavesALogThatStillFoldsAndSaysNothing() {
    Principal alice = Principal.of("alice", "acme");
    turn("c1", "t1", alice);
    turn("c2", "t1", alice);
    turn("c3", "t1", Principal.of("bob", "acme"));

    assertEquals(2, vault.erase("acme/alice"));

    String read = dump(runtime.log().events("c1"));
    assertFalse(read.contains("my card"), read);
    assertFalse(read.contains("under the mat"), read);
    assertTrue(read.contains(Sealer.ERASED), read);
    assertEquals(1, runtime.log().state("c1").turnCount(), "the log still folds");
    assertEquals(TurnStatus.DUPLICATE, turn("c1", "t1", alice).status);
    assertEquals("[erased]", turn("c1", "t1", alice).reply);
    assertTrue(dump(runtime.log().events("c3")).contains("under the mat"), "bob is untouched");
    assertEquals(0, vault.erase("acme/alice"));
  }

  @Test
  void aNewKeyCannotOpenWhatTheOldOneSealed() {
    Principal alice = Principal.of("alice", "acme");
    turn("c1", "t1", alice);
    vault.erase("acme/alice");
    turn("c1", "t2", alice);
    String read = dump(runtime.log().events("c1"));
    assertTrue(read.contains(Sealer.ERASED), read);
    assertTrue(read.contains(TEXT), "the new turn reads under the new key");
    assertEquals(1, read.split("turn_id=t2", -1).length - 1 >= 1 ? 1 : 0);
  }

  @Test
  void anErasedJournalEntryNoLongerOpens() {
    turn("c1", "t1", Principal.of("alice", "acme"));
    vault.erase("acme/alice");
    EffectJournal open = new SealedJournal(journal, vault);
    assertEquals(Sealer.ERASED, open.get(new EffectJournal.Key("c1", "t1", EffectJournal.TOOL, 0)).orElseThrow().result());
  }
}
