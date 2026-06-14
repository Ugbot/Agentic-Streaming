package org.agentic.flink.example.banking.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Pure-logic tests for the LLM-free routing classifier. */
class BankingClassifierTest {

  @Test
  @DisplayName("personal: NEW phase routes a product question to DELEGATE (get CS facts)")
  void personalNewDelegates() {
    assertEquals(
        BankingPath.DELEGATE,
        BankingClassifier.classifyPersonal(BankingPhase.NEW, "Which credit card gives the most cash back?"));
  }

  @Test
  @DisplayName("personal: NEED_INFO routes to GATHER (ask the user)")
  void personalNeedInfoGathers() {
    assertEquals(
        BankingPath.GATHER, BankingClassifier.classifyPersonal(BankingPhase.NEED_INFO, "ok"));
  }

  @Test
  @DisplayName("personal: READY_TO_ACT routes to ACTION")
  void personalReadyActs() {
    assertEquals(
        BankingPath.ACTION,
        BankingClassifier.classifyPersonal(BankingPhase.READY_TO_ACT, "yes, the Gold Rewards Card"));
  }

  @Test
  @DisplayName("personal: explicit 'apply' after NEW context routes to ACTION")
  void personalApplyActs() {
    assertEquals(
        BankingPath.ACTION,
        BankingClassifier.classifyPersonal(BankingPhase.NEED_INFO, "Go ahead and apply for it"));
  }

  @Test
  @DisplayName("either: explicit human request escalates")
  void escalates() {
    assertEquals(
        BankingPath.ESCALATE,
        BankingClassifier.classifyPersonal(BankingPhase.NEW, "I want to speak to a human agent"));
    assertEquals(
        BankingPath.ESCALATE,
        BankingClassifier.classifyCs(BankingPhase.NEW, "let me talk to a person, file a complaint"));
  }

  @Test
  @DisplayName("cs: dispute terms route to DISPUTE")
  void csDispute() {
    assertEquals(
        BankingPath.DISPUTE,
        BankingClassifier.classifyCs(BankingPhase.NEW, "I want to dispute an unauthorized charge"));
  }

  @Test
  @DisplayName("cs: a policy question routes to KNOWLEDGE by default")
  void csKnowledge() {
    assertEquals(
        BankingPath.KNOWLEDGE,
        BankingClassifier.classifyCs(BankingPhase.NEW, "What are the fees on the Blue checking account?"));
  }

  @Test
  @DisplayName("cs: a bank-side action (no question words) routes to ACTION")
  void csAction() {
    assertEquals(
        BankingPath.ACTION,
        BankingClassifier.classifyCs(BankingPhase.NEW, "Please close the account now"));
  }
}
