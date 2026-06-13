package org.agentic.flink.example.banking.graph;

import java.io.Serializable;

/**
 * The envelope that flows between the banking graph operators (router → path → verifier), carrying
 * the A2A turn identity plus the routing decision and the path's result. A plain Flink POJO
 * (public no-arg constructor + getters/setters, POJO-typed fields) so it rides the stream on the
 * POJO serializer without Kryo.
 */
public final class BankingTurn implements Serializable {
  private static final long serialVersionUID = 1L;

  private String taskId;
  private String contextId;
  private String agentId;
  private String userText;
  private BankingPath path; // set by the router
  private String routeReason;
  private String replyText; // set by the path operator
  private boolean actionPerformed; // path signal: an env action was executed this turn
  private boolean blocked; // screening blocked this turn

  public BankingTurn() {}

  public static BankingTurn of(String taskId, String contextId, String agentId, String userText) {
    BankingTurn t = new BankingTurn();
    t.taskId = taskId;
    t.contextId = contextId;
    t.agentId = agentId;
    t.userText = userText;
    return t;
  }

  /** Path label used by {@link org.agentic.flink.graph.RoutedAgentGraph} for fan-out. */
  public String pathName() {
    return path == null ? null : path.name();
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public String getContextId() {
    return contextId;
  }

  public void setContextId(String contextId) {
    this.contextId = contextId;
  }

  public String getAgentId() {
    return agentId;
  }

  public void setAgentId(String agentId) {
    this.agentId = agentId;
  }

  public String getUserText() {
    return userText;
  }

  public void setUserText(String userText) {
    this.userText = userText;
  }

  public BankingPath getPath() {
    return path;
  }

  public void setPath(BankingPath path) {
    this.path = path;
  }

  public String getRouteReason() {
    return routeReason;
  }

  public void setRouteReason(String routeReason) {
    this.routeReason = routeReason;
  }

  public String getReplyText() {
    return replyText;
  }

  public void setReplyText(String replyText) {
    this.replyText = replyText;
  }

  public boolean isActionPerformed() {
    return actionPerformed;
  }

  public void setActionPerformed(boolean actionPerformed) {
    this.actionPerformed = actionPerformed;
  }

  public boolean isBlocked() {
    return blocked;
  }

  public void setBlocked(boolean blocked) {
    this.blocked = blocked;
  }
}
