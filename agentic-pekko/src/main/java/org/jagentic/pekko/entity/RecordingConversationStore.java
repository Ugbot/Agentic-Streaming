package org.jagentic.pekko.entity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jagentic.core.ChatMessage;
import org.jagentic.core.ConversationStore;

/** A per-turn overlay over the entity's committed {@link ConversationStore} view: reads see
 * the committed state plus this turn's pending writes (so the pipeline's mid-turn read-backs —
 * transcript, PATH_ATTR — work), while writes are <b>buffered</b> into a {@link TurnMutations.Recorder}
 * and NOT applied to the committed view. After the turn completes the recorded mutations are
 * persisted as one event and replayed onto the committed view by the event handler. This keeps
 * the committed view clean if the turn fails. */
final class RecordingConversationStore implements ConversationStore {

  private final ConversationStore committed;
  private final TurnMutations.Recorder rec;
  private final List<ChatMessage> pendingAppends = new ArrayList<>();
  private final Map<String, String> pendingAttrs = new LinkedHashMap<>();

  RecordingConversationStore(ConversationStore committed, TurnMutations.Recorder rec) {
    this.committed = committed;
    this.rec = rec;
  }

  @Override
  public void append(String conversationId, ChatMessage message) {
    pendingAppends.add(message);
    rec.append(message);
  }

  @Override
  public List<ChatMessage> history(String conversationId) {
    List<ChatMessage> all = new ArrayList<>(committed.history(conversationId));
    all.addAll(pendingAppends);
    return all;
  }

  @Override
  public int messageCount(String conversationId) {
    return committed.messageCount(conversationId) + pendingAppends.size();
  }

  @Override
  public void putAttribute(String conversationId, String key, String value) {
    pendingAttrs.put(key, value);
    rec.attr(key, value);
  }

  @Override
  public Optional<String> getAttribute(String conversationId, String key) {
    if (pendingAttrs.containsKey(key)) {
      return Optional.ofNullable(pendingAttrs.get(key));
    }
    return committed.getAttribute(conversationId, key);
  }

  @Override
  public Map<String, String> attributes(String conversationId) {
    Map<String, String> merged = new LinkedHashMap<>(committed.attributes(conversationId));
    merged.putAll(pendingAttrs);
    return merged;
  }

  @Override
  public void associateUser(String conversationId, String userId) {
    rec.associate(userId);
  }

  @Override
  public List<String> conversationsForUser(String userId) {
    return committed.conversationsForUser(userId);
  }

  @Override
  public void clear(String conversationId) {
    // not expected mid-turn; clearing is a control-plane op handled by the entity directly
    committed.clear(conversationId);
  }
}
