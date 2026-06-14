package org.jagentic.core;

import java.util.List;
import java.util.regex.Pattern;

/** Blocks when any deny-pattern matches (case-insensitive). Mirrors the injection /
 * prohibited-content screen in the banking example. */
public final class RegexGuardrail implements Guardrail {
  private final List<Pattern> patterns;
  private final String reason;
  private final boolean checkOutputs;

  public RegexGuardrail(List<String> deny, String reason, boolean checkOutputs) {
    this.patterns = deny.stream().map(p -> Pattern.compile(p, Pattern.CASE_INSENSITIVE)).toList();
    this.reason = reason;
    this.checkOutputs = checkOutputs;
  }

  public RegexGuardrail(List<String> deny, String reason) {
    this(deny, reason, false);
  }

  private String hit(String text) {
    if (text != null) {
      for (Pattern p : patterns) {
        if (p.matcher(text).find()) {
          return reason;
        }
      }
    }
    return null;
  }

  @Override
  public String checkInput(String text) {
    return hit(text);
  }

  @Override
  public String checkOutput(String reply) {
    return checkOutputs ? hit(reply) : null;
  }
}
