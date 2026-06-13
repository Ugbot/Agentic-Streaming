package org.agentic.flink.a2a.gateway;

import io.a2a.spec.DataPart;
import io.a2a.spec.Message;
import io.a2a.spec.Part;
import io.a2a.spec.TextPart;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.agentic.flink.a2a.A2AMessage;
import org.agentic.flink.a2a.A2APart;

/** Maps between the A2A SDK ({@code io.a2a.spec}) types and our framework value model. */
final class GatewayMapping {

  private GatewayMapping() {}

  /** SDK inbound {@link Message} → our {@link A2AMessage}. */
  static A2AMessage toModel(Message message) {
    List<A2APart> parts = new ArrayList<>();
    if (message.parts() != null) {
      for (Part<?> p : message.parts()) {
        if (p instanceof TextPart) {
          parts.add(A2APart.text(((TextPart) p).text()));
        } else if (p instanceof DataPart) {
          Object data = ((DataPart) p).data();
          if (data instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) data;
            parts.add(A2APart.data(m));
          } else {
            parts.add(A2APart.data(Map.of("value", data)));
          }
        } else {
          parts.add(A2APart.data(Map.of("kind", p.getClass().getSimpleName(), "value", String.valueOf(p))));
        }
      }
    }
    A2AMessage.Role role =
        message.role() == Message.Role.ROLE_AGENT ? A2AMessage.Role.AGENT : A2AMessage.Role.USER;
    String messageId = message.messageId() != null ? message.messageId() : UUID.randomUUID().toString();
    return new A2AMessage(
        role, messageId, parts, message.contextId(), message.taskId(), message.metadata());
  }

  /** Our {@link A2APart}s → SDK {@link Part}s (for emitted artifacts). */
  static List<Part<?>> toSdkParts(List<A2APart> parts) {
    List<Part<?>> out = new ArrayList<>();
    for (A2APart part : parts) {
      switch (part.getKind()) {
        case TEXT:
          out.add(new TextPart(part.getText() == null ? "" : part.getText()));
          break;
        case DATA:
          out.add(new DataPart(part.getData() == null ? Map.of() : part.getData()));
          break;
        case FILE:
          out.add(new DataPart(fileMap(part)));
          break;
        default:
          break;
      }
    }
    return out;
  }

  private static Map<String, Object> fileMap(A2APart part) {
    java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
    if (part.getFileUri() != null) {
      m.put("uri", part.getFileUri());
    }
    if (part.getFileBytes() != null) {
      m.put("bytes", part.getFileBytes());
    }
    if (part.getMimeType() != null) {
      m.put("mimeType", part.getMimeType());
    }
    return m;
  }

  /** A text {@link Message} in the agent role (for status updates). */
  static Message agentText(String text) {
    return Message.builder()
        .role(Message.Role.ROLE_AGENT)
        .parts(List.of(new TextPart(text == null ? "" : text)))
        .messageId(UUID.randomUUID().toString())
        .build();
  }
}
