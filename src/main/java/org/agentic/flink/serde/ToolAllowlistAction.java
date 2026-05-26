package org.agentic.flink.serde;

public enum ToolAllowlistAction {
  ADD, // Add tools to allowlist
  REMOVE, // Remove tools from allowlist
  REPLACE, // Replace entire allowlist
  CLEAR // Clear all tools (emergency lockdown)
}
