package org.jagentic.core;

import java.util.List;
import java.util.regex.Pattern;

/** Blocks when any allow-pattern fails to match or any deny-pattern matches (case-insensitive). */
public final class RegexGuardrail implements Guardrail {
  private final List<Pattern> allow;
  private final List<Pattern> patterns;
  private final String reason;
  private final boolean checkOutputs;

  public RegexGuardrail(List<String> deny, String reason, boolean checkOutputs) {
    this(List.of(), deny, reason, checkOutputs);
  }

  public RegexGuardrail(List<String> allow, List<String> deny, String reason, boolean checkOutputs) {
    this.allow = allow.stream().map(p -> Pattern.compile(p, Pattern.CASE_INSENSITIVE)).toList();
    this.patterns = deny.stream().map(p -> Pattern.compile(p, Pattern.CASE_INSENSITIVE)).toList();
    this.reason = reason;
    this.checkOutputs = checkOutputs;
  }

  public RegexGuardrail(List<String> deny, String reason) {
    this(deny, reason, false);
  }

  private String hit(String text) {
    for (Pattern p : allow) {
      if (text == null || !p.matcher(text).find()) {
        return reason;
      }
    }
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
