package org.agentic.flink.serde;

public enum ControlCommandType {
  CLEAR_CONTEXT, // Clear chat memory state
  RESET_AGENT, // Reset agent to initial state
  UPDATE_TOOLS, // Update available tools
  SET_MAX_ITERATIONS, // Change loop limit
  FORCE_STOP, // Stop current execution
  PAUSE, // Pause processing
  RESUME, // Resume processing
  EVICT_STATE, // Force state eviction
  UPDATE_CONFIG // Update agent configuration
}
