package org.agentic.flink.llm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The shared ReAct action-adherence guard. These assertions pin the heuristic so both the core
 * {@link org.agentic.flink.function.ReActProcessFunction} operator and the banking brain behave
 * consistently: stall finals are caught, genuine answers are not.
 */
class ReActGuardTest {

  @Test
  @DisplayName("tool-stall phrasing is detected")
  void detectsStalls() {
    assertTrue(ReActGuard.looksLikeToolStall("I need to inspect the available tools first."));
    assertTrue(ReActGuard.looksLikeToolStall("I don't have access to the required tools."));
    assertTrue(ReActGuard.looksLikeToolStall("I'll submit it as soon as tool access is available."));
    assertTrue(ReActGuard.looksLikeToolStall("I'm unable to call the tool right now."));
  }

  @Test
  @DisplayName("genuine / knowledge answers are NOT treated as stalls")
  void ignoresGenuineAnswers() {
    assertFalse(
        ReActGuard.looksLikeToolStall(
            "The Gold Rewards Card has 2.5% cash back and no annual fee."));
    assertFalse(ReActGuard.looksLikeToolStall("Done — I've submitted the application."));
    assertFalse(ReActGuard.looksLikeToolStall(""));
    assertFalse(ReActGuard.looksLikeToolStall(null));
  }

  @Test
  @DisplayName("shouldNudge only fires when tools exist, none were called, and within the budget")
  void shouldNudgeConditions() {
    String stall = "I need to inspect the tools first.";
    String real = "Here is your balance: $42.";
    // Fires: has tools, no calls yet, under budget, stall text.
    assertTrue(ReActGuard.shouldNudge(stall, true, 0, 0));
    // No tools available -> can't act anyway -> accept the final.
    assertFalse(ReActGuard.shouldNudge(stall, false, 0, 0));
    // Already called a tool this run -> not a stall, accept.
    assertFalse(ReActGuard.shouldNudge(stall, true, 1, 0));
    // Nudge budget exhausted -> accept to avoid looping.
    assertFalse(ReActGuard.shouldNudge(stall, true, 0, ReActGuard.MAX_STALL_NUDGES));
    // Genuine answer -> never nudge.
    assertFalse(ReActGuard.shouldNudge(real, true, 0, 0));
  }

  @Test
  @DisplayName("stallNudge lists the available tools so the model knows what to call")
  void nudgeListsTools() {
    String nudge = ReActGuard.stallNudge(List.of("list_env_tools", "call_env_tool"));
    assertTrue(nudge.contains("list_env_tools"));
    assertTrue(nudge.contains("call_env_tool"));
    assertTrue(nudge.toLowerCase().contains("action"));
  }
}
