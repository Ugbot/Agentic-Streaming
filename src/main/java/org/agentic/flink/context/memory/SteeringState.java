package org.agentic.flink.context.memory;

import org.agentic.flink.context.core.ContextPriority;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Steering state for agent behavior MoSCoW rules and constraints that guide agent decisions
 *
 * <p>MUST: Hard constraints, non-negotiable SHOULD: Strong preferences COULD: Nice to have WONT:
 * Explicitly avoid
 */
@Data
public class SteeringState implements Serializable {

  private Map<String, SteeringRule> rules; // key = rule_id
  private long lastUpdatedAt;

  public SteeringState() {
    this.rules = new HashMap<>();
    this.lastUpdatedAt = System.currentTimeMillis();
  }

  public void addRule(SteeringRule rule) {
    rules.put(rule.getRuleId(), rule);
    lastUpdatedAt = System.currentTimeMillis();
  }

  public void addMust(String ruleId, String description, String constraint) {
    addRule(new SteeringRule(ruleId, description, constraint, ContextPriority.MUST));
  }

  public void addShould(String ruleId, String description, String constraint) {
    addRule(new SteeringRule(ruleId, description, constraint, ContextPriority.SHOULD));
  }

  public void addCould(String ruleId, String description, String constraint) {
    addRule(new SteeringRule(ruleId, description, constraint, ContextPriority.COULD));
  }

  public void addWont(String ruleId, String description, String constraint) {
    addRule(new SteeringRule(ruleId, description, constraint, ContextPriority.WONT));
  }

  public SteeringRule getRule(String ruleId) {
    return rules.get(ruleId);
  }

  public void removeRule(String ruleId) {
    rules.remove(ruleId);
    lastUpdatedAt = System.currentTimeMillis();
  }

  public List<SteeringRule> getRulesByPriority(ContextPriority priority) {
    return rules.values().stream()
        .filter(rule -> rule.getPriority() == priority)
        .collect(Collectors.toList());
  }

  public List<SteeringRule> getMustRules() {
    return getRulesByPriority(ContextPriority.MUST);
  }

  public List<SteeringRule> getShouldRules() {
    return getRulesByPriority(ContextPriority.SHOULD);
  }

  public List<SteeringRule> getCouldRules() {
    return getRulesByPriority(ContextPriority.COULD);
  }

  public List<SteeringRule> getWontRules() {
    return getRulesByPriority(ContextPriority.WONT);
  }

  public List<SteeringRule> getAllRules() {
    return new ArrayList<>(rules.values());
  }

  public int size() {
    return rules.size();
  }

  public boolean isEmpty() {
    return rules.isEmpty();
  }

  public ContextPriority getPriority(String itemContent) {
    // Check if content matches any rules
    for (SteeringRule rule : rules.values()) {
      if (rule.matches(itemContent)) {
        return rule.getPriority();
      }
    }
    // Default to COULD if no rule matches
    return ContextPriority.COULD;
  }

  @Override
  public String toString() {
    return String.format(
        "SteeringState[rules=%d (MUST:%d, SHOULD:%d, COULD:%d, WONT:%d)]",
        rules.size(),
        getMustRules().size(),
        getShouldRules().size(),
        getCouldRules().size(),
        getWontRules().size());
  }

  /** Individual steering rule */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SteeringRule implements Serializable {
    private String ruleId;
    private String description;
    private String constraint;
    private ContextPriority priority;
    private long createdAt;

    public SteeringRule(
        String ruleId, String description, String constraint, ContextPriority priority) {
      this.ruleId = ruleId;
      this.description = description;
      this.constraint = constraint;
      this.priority = priority;
      this.createdAt = System.currentTimeMillis();
    }

    public boolean matches(String content) {
      // Simple substring match for now
      // Could be enhanced with regex or semantic matching
      return content.toLowerCase().contains(constraint.toLowerCase());
    }
  }
}
