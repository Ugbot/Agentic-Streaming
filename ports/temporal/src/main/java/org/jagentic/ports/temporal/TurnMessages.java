package org.jagentic.ports.temporal;

/**
 * Plain Jackson-serializable payloads for the workflow Update method (Temporal's
 * default data converter is JSON/Jackson). Kept as simple classes with a no-arg
 * constructor so serialization is unambiguous across SDK/Jackson versions.
 */
public final class TurnMessages {

  private TurnMessages() {}

  /** One inbound conversational turn delivered to the entity workflow. */
  public static final class TurnRequest {
    public String text;
    public String userId;

    public TurnRequest() {}

    public TurnRequest(String text, String userId) {
      this.text = text;
      this.userId = userId;
    }
  }

  /** The verified reply for a turn, plus the durable transcript size after it. */
  public static final class TurnReply {
    public String reply;
    public String path;
    public boolean ok;
    public int messageCount;

    public TurnReply() {}

    public TurnReply(String reply, String path, boolean ok, int messageCount) {
      this.reply = reply;
      this.path = path;
      this.ok = ok;
      this.messageCount = messageCount;
    }
  }
}
