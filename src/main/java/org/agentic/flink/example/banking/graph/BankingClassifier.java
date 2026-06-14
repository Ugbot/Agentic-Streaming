package org.agentic.flink.example.banking.graph;

import java.util.Locale;

/**
 * Rule-based router logic for the banking agents — pure, deterministic, and <b>LLM-free</b> so the
 * router adds no model calls per turn (respecting the harness's 5-min/turn limit and single-Vertex
 * 429 ceiling). Decides the {@link BankingPath} from the conversation {@link BankingPhase} plus
 * light keyword heuristics on the message text.
 *
 * <p>Personal-agent flow chains across turns: {@code NEW → DELEGATE} (get product facts) →
 * {@code NEED_INFO → GATHER} (ask the user) → {@code READY_TO_ACT → ACTION} (perform). CS-agent
 * routes by intent: dispute → {@code DISPUTE}, action → {@code ACTION}, else {@code KNOWLEDGE}.
 * Either agent escalates on explicit human/complaint requests.
 */
public final class BankingClassifier {

  private BankingClassifier() {}

  private static final String[] ESCALATE_TERMS = {
    "speak to a human", "human agent", "real person", "talk to a person",
    "file a complaint", "lodge a complaint", "manager", "supervisor"
  };
  private static final String[] DISPUTE_TERMS = {
    "dispute", "chargeback", "charge back", "unauthorized", "unauthorised",
    "fraud", "didn't make this", "did not make this", "wrong charge"
  };
  private static final String[] ACTION_TERMS = {
    "apply", "go ahead", "do it", "proceed", "submit it", "yes please do",
    "open the", "close the", "transfer", "set it up", "sign me up", "go for it"
  };
  private static final String[] FACT_TERMS = {
    "which", "what", "how much", "rate", "fee", "fees", "cash back", "cashback",
    "interest", "eligib", "compare", "options", "best card", "recommend"
  };

  /** Decide the path for the personal agent. */
  public static BankingPath classifyPersonal(BankingPhase phase, String text) {
    String t = lower(text);
    if (containsAny(t, ESCALATE_TERMS)) {
      return BankingPath.ESCALATE;
    }
    if (phase == BankingPhase.READY_TO_ACT || phase == BankingPhase.ACTED) {
      return BankingPath.ACTION;
    }
    if (containsAny(t, ACTION_TERMS) && phase != BankingPhase.NEW) {
      return BankingPath.ACTION; // user told us to proceed and we already have context
    }
    if (phase == BankingPhase.NEED_INFO) {
      return BankingPath.GATHER; // ask the user for the missing details
    }
    // NEW (or default): first get the product/policy facts from customer service.
    return BankingPath.DELEGATE;
  }

  /** Decide the path for the customer-service agent. */
  public static BankingPath classifyCs(BankingPhase phase, String text) {
    String t = lower(text);
    if (containsAny(t, DISPUTE_TERMS)) {
      return BankingPath.DISPUTE;
    }
    if (containsAny(t, ESCALATE_TERMS)) {
      return BankingPath.ESCALATE;
    }
    if (containsAny(t, ACTION_TERMS) && !containsAny(t, FACT_TERMS)) {
      return BankingPath.ACTION; // a bank-side action request, not a question
    }
    return BankingPath.KNOWLEDGE; // default: answer from the knowledge base
  }

  /** Decide the path for either agent. */
  public static BankingPath classify(boolean personal, BankingPhase phase, String text) {
    return personal ? classifyPersonal(phase, text) : classifyCs(phase, text);
  }

  private static String lower(String s) {
    return s == null ? "" : s.toLowerCase(Locale.ROOT);
  }

  private static boolean containsAny(String haystack, String[] needles) {
    for (String n : needles) {
      if (haystack.contains(n)) {
        return true;
      }
    }
    return false;
  }
}
