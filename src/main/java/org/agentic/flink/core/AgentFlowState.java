package org.agentic.flink.core;

public enum AgentFlowState {
  ACTIVE, // Currently processing
  WAITING_USER_INPUT, // Waiting for user response
  WAITING_TOOL, // Waiting for tool execution
  WAITING_APPROVAL, // Waiting for supervisor approval
  PAUSED, // Manually paused
  TIMEOUT_PENDING, // About to be offloaded
  OFFLOADED, // Saved to external storage
  COMPLETED, // Successfully completed
  FAILED // Failed and archived
}
