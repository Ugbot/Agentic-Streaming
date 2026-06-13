package org.agentic.flink.a2a;

import io.a2a.client.Client;
import io.a2a.client.ClientEvent;
import io.a2a.client.MessageEvent;
import io.a2a.client.TaskEvent;
import io.a2a.client.TaskUpdateEvent;
import io.a2a.client.config.ClientConfig;
import io.a2a.client.http.A2ACardResolver;
import io.a2a.client.http.JdkA2AHttpClient;
import io.a2a.client.transport.jsonrpc.JSONRPCTransport;
import io.a2a.client.transport.jsonrpc.JSONRPCTransportConfigBuilder;
import io.a2a.spec.AgentCapabilities;
import io.a2a.spec.AgentInterface;
import io.a2a.spec.DataPart;
import io.a2a.spec.Message;
import io.a2a.spec.Part;
import io.a2a.spec.Task;
import io.a2a.spec.TaskIdParams;
import io.a2a.spec.TaskQueryParams;
import io.a2a.spec.TaskStatus;
import io.a2a.spec.TextPart;
import io.a2a.spec.TransportProtocol;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link A2AClient} backed by the official {@code a2a-java} SDK (JSON-RPC transport).
 *
 * <p>This is the single class that touches SDK types ({@code io.a2a.*}); the rest of the codebase
 * works against our SPI and {@code A2A*} value types. It translates between our value model and the
 * SDK records, and adapts the SDK's event-callback {@link Client} into the synchronous {@code
 * send}/{@code getTask}/{@code cancel}/{@code stream} our SPI exposes.
 *
 * <p>Built lazily on the task side (the SDK {@link Client} is not {@link java.io.Serializable}),
 * via {@link SdkA2AClientFactory}. Only the JSON-RPC binding is wired here — gRPC/REST require their
 * own SDK transport modules and are reported as unsupported rather than silently downgraded.
 */
public final class SdkA2AClient implements A2AClient {
  private static final Logger LOG = LoggerFactory.getLogger(SdkA2AClient.class);

  private final RemoteAgentSpec spec;
  private final io.a2a.spec.AgentCard sdkCard;
  private final Client client;

  SdkA2AClient(RemoteAgentSpec spec) {
    if (spec.transport() != A2ATransport.JSONRPC) {
      throw new A2AClientException(
          "SdkA2AClient currently wires only the JSONRPC transport; peer '"
              + spec.name()
              + "' requests "
              + spec.transport()
              + ". Add the matching a2a-java transport module and extend SdkA2AClient to support it.");
    }
    this.spec = spec;
    try {
      this.sdkCard = resolveCard(spec);
      ClientConfig config =
          new ClientConfig.Builder()
              .setStreaming(spec.streaming())
              .setPolling(!spec.streaming())
              .build();
      this.client =
          Client.builder(sdkCard)
              .withTransport(
                  JSONRPCTransport.class,
                  new JSONRPCTransportConfigBuilder().httpClient(new JdkA2AHttpClient()))
              .clientConfig(config)
              .build();
    } catch (Exception e) {
      throw new A2AClientException("Failed to build A2A SDK client for peer " + spec.name(), e);
    }
  }

  @Override
  public RemoteAgentSpec spec() {
    return spec;
  }

  @Override
  public A2AAgentCard fetchCard() {
    return toModelCard(sdkCard);
  }

  @Override
  public A2ATask send(A2AMessage message) {
    AtomicReference<A2ATask> result = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    BiConsumer<ClientEvent, io.a2a.spec.AgentCard> consumer =
        (event, card) -> {
          A2ATask mapped = mapEvent(event);
          if (mapped != null) {
            result.compareAndSet(null, mapped);
            latch.countDown();
          }
        };
    Consumer<Throwable> onError =
        t -> {
          error.set(t);
          latch.countDown();
        };

    try {
      client.sendMessage(toSdkMessage(message), List.of(consumer), onError, null);
      if (!latch.await(spec.requestTimeoutMs(), TimeUnit.MILLISECONDS)) {
        throw new A2AClientException("Timed out awaiting first A2A event from peer " + spec.name());
      }
    } catch (io.a2a.spec.A2AClientException e) {
      throw new A2AClientException("A2A sendMessage failed for peer " + spec.name(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new A2AClientException("Interrupted sending to peer " + spec.name(), e);
    }
    if (error.get() != null) {
      throw new A2AClientException("A2A sendMessage error from peer " + spec.name(), error.get());
    }
    A2ATask task = result.get();
    if (task == null) {
      throw new A2AClientException("Peer " + spec.name() + " returned no task or message");
    }
    return task;
  }

  @Override
  public A2ATask getTask(String taskId) {
    try {
      Task task = client.getTask(new TaskQueryParams(taskId), null);
      return toModelTask(task);
    } catch (io.a2a.spec.A2AClientException e) {
      throw new A2AClientException("tasks/get failed for " + taskId, e);
    }
  }

  @Override
  public A2ATask cancel(String taskId) {
    try {
      Task task = client.cancelTask(new TaskIdParams(taskId), null);
      return toModelTask(task);
    } catch (io.a2a.spec.A2AClientException e) {
      throw new A2AClientException("tasks/cancel failed for " + taskId, e);
    }
  }

  @Override
  public A2ATask stream(A2AMessage message, java.util.function.Consumer<A2ATask> onUpdate) {
    AtomicReference<A2ATask> last = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);

    BiConsumer<ClientEvent, io.a2a.spec.AgentCard> consumer =
        (event, card) -> {
          A2ATask mapped = mapEvent(event);
          if (mapped != null) {
            last.set(mapped);
            onUpdate.accept(mapped);
            if (mapped.getState().isFinal()) {
              done.countDown();
            }
          }
        };
    Consumer<Throwable> onError =
        t -> {
          error.set(t);
          done.countDown();
        };

    try {
      client.sendMessage(toSdkMessage(message), List.of(consumer), onError, null);
      if (!done.await(spec.requestTimeoutMs(), TimeUnit.MILLISECONDS)) {
        throw new A2AClientException("Timed out streaming from peer " + spec.name());
      }
    } catch (io.a2a.spec.A2AClientException e) {
      throw new A2AClientException("A2A message/stream failed for peer " + spec.name(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new A2AClientException("Interrupted streaming from peer " + spec.name(), e);
    }
    if (error.get() != null) {
      throw new A2AClientException("A2A stream error from peer " + spec.name(), error.get());
    }
    return last.get();
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (RuntimeException e) {
      LOG.debug("Error closing A2A client for {}: {}", spec.name(), e.getMessage());
    }
  }

  // ==================== Card resolution ====================

  private static io.a2a.spec.AgentCard resolveCard(RemoteAgentSpec spec) throws Exception {
    if (spec.usesCardDiscovery()) {
      URI uri = URI.create(spec.agentCardUrl());
      String base = uri.getScheme() + "://" + uri.getAuthority();
      String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
      return new A2ACardResolver(new JdkA2AHttpClient(), base, path).getAgentCard();
    }
    // Pinned endpoint: synthesize a minimal card declaring the JSON-RPC interface.
    return io.a2a.spec.AgentCard.builder()
        .name(spec.name())
        .description(spec.description() == null ? "" : spec.description())
        .version("0.0.0")
        .capabilities(new AgentCapabilities(spec.streaming(), false, false, List.of()))
        .defaultInputModes(List.of("text/plain", "application/json"))
        .defaultOutputModes(List.of("text/plain", "application/json"))
        .skills(List.of())
        .supportedInterfaces(
            List.of(new AgentInterface(TransportProtocol.JSONRPC.asString(), spec.endpointUrl())))
        .build();
  }

  // ==================== Mapping: SDK -> model ====================

  private A2ATask mapEvent(ClientEvent event) {
    if (event instanceof TaskEvent) {
      return toModelTask(((TaskEvent) event).getTask());
    }
    if (event instanceof TaskUpdateEvent) {
      return toModelTask(((TaskUpdateEvent) event).getTask());
    }
    if (event instanceof MessageEvent) {
      return messageAsCompletedTask(((MessageEvent) event).getMessage());
    }
    return null;
  }

  private A2ATask toModelTask(Task task) {
    long now = System.currentTimeMillis();
    TaskStatus status = task.status();
    A2ATaskState state =
        status == null || status.state() == null
            ? A2ATaskState.UNKNOWN
            : A2ATaskState.fromWire(status.state().name());
    String statusMessage =
        status != null && status.message() != null ? textOf(status.message().parts()) : null;

    List<A2AArtifact> artifacts = new ArrayList<>();
    if (task.artifacts() != null) {
      for (io.a2a.spec.Artifact a : task.artifacts()) {
        artifacts.add(
            new A2AArtifact(
                a.artifactId(), a.name(), a.description(), toModelParts(a.parts()), a.metadata()));
      }
    }
    List<A2AMessage> history = new ArrayList<>();
    if (task.history() != null) {
      for (Message m : task.history()) {
        history.add(toModelMessage(m));
      }
    }
    return new A2ATask(
        task.id(), task.contextId(), state, statusMessage, history, artifacts, task.metadata(), now, now);
  }

  private A2ATask messageAsCompletedTask(Message message) {
    long now = System.currentTimeMillis();
    A2AArtifact artifact =
        new A2AArtifact(
            message.messageId(), "message", null, toModelParts(message.parts()), null);
    String taskId = message.taskId() != null ? message.taskId() : message.messageId();
    return new A2ATask(
        taskId,
        message.contextId(),
        A2ATaskState.COMPLETED,
        null,
        List.of(toModelMessage(message)),
        List.of(artifact),
        null,
        now,
        now);
  }

  private A2AMessage toModelMessage(Message m) {
    A2AMessage.Role role =
        m.role() == Message.Role.ROLE_AGENT ? A2AMessage.Role.AGENT : A2AMessage.Role.USER;
    return new A2AMessage(
        role, m.messageId(), toModelParts(m.parts()), m.contextId(), m.taskId(), m.metadata());
  }

  private List<A2APart> toModelParts(List<Part<?>> parts) {
    List<A2APart> out = new ArrayList<>();
    if (parts == null) {
      return out;
    }
    for (Part<?> part : parts) {
      if (part instanceof TextPart) {
        out.add(A2APart.text(((TextPart) part).text()));
      } else if (part instanceof DataPart) {
        Object data = ((DataPart) part).data();
        if (data instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> m = (Map<String, Object>) data;
          out.add(A2APart.data(m));
        } else {
          out.add(A2APart.data(Map.of("value", data)));
        }
      } else {
        // FilePart or unknown — represent structurally so nothing is silently dropped.
        out.add(A2APart.data(Map.of("kind", part.getClass().getSimpleName(), "value", String.valueOf(part))));
      }
    }
    return out;
  }

  private static String textOf(List<Part<?>> parts) {
    if (parts == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Part<?> p : parts) {
      if (p instanceof TextPart) {
        if (sb.length() > 0) {
          sb.append('\n');
        }
        sb.append(((TextPart) p).text());
      }
    }
    return sb.length() == 0 ? null : sb.toString();
  }

  private A2AAgentCard toModelCard(io.a2a.spec.AgentCard card) {
    A2AAgentCard.Builder b =
        A2AAgentCard.builder()
            .name(card.name())
            .description(card.description())
            .version(card.version())
            .defaultInputModes(card.defaultInputModes())
            .defaultOutputModes(card.defaultOutputModes());
    AgentCapabilities caps = card.capabilities();
    if (caps != null) {
      b.capabilities(caps.streaming(), caps.pushNotifications(), caps.extendedAgentCard());
    }
    if (card.supportedInterfaces() != null && !card.supportedInterfaces().isEmpty()) {
      boolean first = true;
      for (AgentInterface iface : card.supportedInterfaces()) {
        A2ATransport t = A2ATransport.fromWire(iface.protocolBinding());
        if (first) {
          b.url(iface.url()).preferredTransport(t);
          first = false;
        } else {
          b.addInterface(iface.url(), t);
        }
      }
    }
    if (card.skills() != null) {
      for (io.a2a.spec.AgentSkill s : card.skills()) {
        b.addSkill(
            new A2AAgentSkill(
                s.id(), s.name(), s.description(), s.tags(), s.examples(), s.inputModes(), s.outputModes()));
      }
    }
    return b.build();
  }

  // ==================== Mapping: model -> SDK ====================

  private Message toSdkMessage(A2AMessage message) {
    List<Part<?>> parts = new ArrayList<>();
    for (A2APart part : message.getParts()) {
      switch (part.getKind()) {
        case TEXT:
          parts.add(new TextPart(part.getText() == null ? "" : part.getText()));
          break;
        case DATA:
          parts.add(new DataPart(part.getData() == null ? Map.of() : part.getData()));
          break;
        case FILE:
          Map<String, Object> file = new LinkedHashMap<>();
          if (part.getFileUri() != null) {
            file.put("uri", part.getFileUri());
          }
          if (part.getFileBytes() != null) {
            file.put("bytes", part.getFileBytes());
          }
          if (part.getMimeType() != null) {
            file.put("mimeType", part.getMimeType());
          }
          parts.add(new DataPart(file));
          break;
        default:
          break;
      }
    }
    Message.Builder b =
        Message.builder()
            .role(Message.Role.ROLE_USER)
            .parts(parts)
            .messageId(message.getMessageId());
    if (message.getContextId() != null) {
      b.contextId(message.getContextId());
    }
    if (message.getTaskId() != null) {
      b.taskId(message.getTaskId());
    }
    return b.build();
  }
}
