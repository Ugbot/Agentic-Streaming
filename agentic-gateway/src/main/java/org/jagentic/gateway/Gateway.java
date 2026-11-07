package org.jagentic.gateway;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.jagentic.core.Event;
import org.jagentic.core.EventType;
import org.jagentic.core.LogEvent;
import org.jagentic.core.Principal;
import org.jagentic.core.TurnResult;
import org.jagentic.gateway.auth.Unauthenticated;

/**
 * The one HTTP front door of the canonical path.
 *
 * <ul>
 *   <li>{@code POST /agent} runs a turn: {@code conversation_id}, optional {@code turn_id}, {@code
 *       text}, optional {@code metadata}.
 *   <li>{@code POST /conversations/{id}/turns/{turn}/resume} answers a suspended turn; the body is
 *       the signal.
 *   <li>{@code GET /conversations/{id}} reads the log.
 *   <li>{@code GET /healthz} and {@code GET /.well-known/agent-card.json} are the only routes open
 *       without a token, and say nothing about any conversation.
 * </ul>
 *
 * <p>Who is calling comes from the token and from nowhere else: {@code user_id}, {@code principal}
 * and {@code tenant} in a body are not read. A conversation is keyed by the caller's tenant, so
 * another tenant cannot even name it, and within a tenant only its owner, or a reader role, gets at
 * it. Someone else's conversation answers 404, the same as one that does not exist. A turn stopped
 * by a limit is a 429, and retrying it needs a new {@code turn_id}.
 */
public final class Gateway implements AutoCloseable {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};
  private static final int MAX_BODY = 1 << 20;
  private static final byte[] NOT_FOUND = "{\"error\":\"not found\"}".getBytes(StandardCharsets.UTF_8);

  private final GatewayOptions options;
  private final TurnBackend backend;
  private final Map<String, Object> card;
  private final HttpServer server;
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
  /**
   * Who sent the first turn of a conversation the backend does not show yet. A Flink job's log is
   * seen only through its results, so until the first one comes back the conversation looks empty,
   * and without this anyone in the tenant could put a turn into it.
   */
  private final Map<String, Principal> claims = new java.util.concurrent.ConcurrentHashMap<>();

  private Gateway(GatewayOptions options, TurnBackend backend, Map<String, Object> card) throws IOException {
    this.options = options;
    this.backend = backend;
    this.card = card;
    this.server = HttpServer.create(new InetSocketAddress(options.host(), options.port()), 0);
    server.setExecutor(workers);
    server.createContext("/", this::handle);
  }

  /** Binds and starts serving. */
  public static Gateway start(GatewayOptions options, TurnBackend backend, Map<String, Object> card)
      throws IOException {
    Gateway gateway = new Gateway(options, backend, card);
    gateway.server.start();
    return gateway;
  }

  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    server.stop(0);
    workers.shutdown();
  }

  /** The key a conversation has on the runtime: encoded, so no pair of names can collide. */
  public static String scopedKey(String tenant, String conversationId) {
    return URLEncoder.encode(tenant, StandardCharsets.UTF_8) + "/"
        + URLEncoder.encode(conversationId, StandardCharsets.UTF_8);
  }

  private void handle(HttpExchange x) throws IOException {
    try {
      route(x);
    } catch (Refusal r) {
      send(x, r.status, r.body);
    } catch (JsonProcessingException | ClassCastException e) {
      send(x, 400, error("malformed request"));
    } catch (RuntimeException e) {
      send(x, 500, error("internal error"));
    } finally {
      x.close();
    }
  }

  private void route(HttpExchange x) throws IOException, Refusal {
    String method = x.getRequestMethod();
    String[] parts = x.getRequestURI().getRawPath().substring(1).split("/", -1);
    if (parts.length == 1 && parts[0].equals("healthz") && method.equals("GET")) {
      send(x, 200, JSON.writeValueAsBytes(Map.of("status", "ok")));
    } else if (x.getRequestURI().getRawPath().equals("/.well-known/agent-card.json") && method.equals("GET")) {
      send(x, 200, JSON.writeValueAsBytes(card));
    } else if (parts.length == 1 && parts[0].equals("agent") && method.equals("POST")) {
      Principal who = authenticate(x);
      submitTurn(x, who);
    } else if (parts.length == 2 && parts[0].equals("conversations") && method.equals("GET")) {
      Principal who = authenticate(x);
      readConversation(x, who, decode(parts[1]));
    } else if (parts.length == 5 && parts[0].equals("conversations") && parts[2].equals("turns")
        && parts[4].equals("resume") && method.equals("POST")) {
      Principal who = authenticate(x);
      resume(x, who, decode(parts[1]), decode(parts[3]));
    } else {
      send(x, 404, NOT_FOUND);
    }
  }

  /** A path segment's percent escapes, and nothing else: a {@code +} is a plus, as in the body. */
  private static String decode(String part) throws Refusal {
    try {
      return URLDecoder.decode(part.replace("+", "%2B"), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      throw new Refusal(400, error("malformed path"));
    }
  }

  private Principal authenticate(HttpExchange x) throws Refusal {
    String header = x.getRequestHeaders().getFirst("Authorization");
    if (header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7)) {
      x.getResponseHeaders().set("WWW-Authenticate", "Bearer");
      throw new Refusal(401, error("a bearer token is required"));
    }
    Principal who;
    try {
      who = options.authenticator().authenticate(header.substring(7).trim());
    } catch (Unauthenticated e) {
      x.getResponseHeaders().set("WWW-Authenticate", "Bearer error=\"invalid_token\"");
      throw new Refusal(401, error("invalid token"));
    }
    if (who.tenant() == null || who.tenant().isBlank()) {
      throw new Refusal(403, error("the token names no tenant"));
    }
    return who;
  }

  private void submitTurn(HttpExchange x, Principal who) throws IOException, Refusal {
    Map<String, Object> body = body(x);
    String conversationId = required(body, "conversation_id");
    String key = scopedKey(who.tenant(), conversationId);
    boolean claimed = requireAccess(who, key, false).isEmpty();
    if (claimed) {
      Principal first = claims.putIfAbsent(key, who);
      if (first != null && !sameOwner(first, who)) {
        throw new Refusal(404, NOT_FOUND);
      }
    }
    String turnId = body.get("turn_id") instanceof String t && !t.isBlank() ? t : UUID.randomUUID().toString();
    String text = required(body, "text");
    Map<String, String> metadata = new LinkedHashMap<>();
    if (body.get("metadata") instanceof Map<?, ?> m) {
      m.forEach((k, v) -> metadata.put(String.valueOf(k), String.valueOf(v)));
    }
    TurnResult result;
    try {
      result = await(backend.submit(new Event(key, turnId, who.subject(), text, metadata, null, who)));
    } catch (Refusal r) {
      if (claimed && r.status != 504) {
        claims.remove(key, who); // nothing is running, so nothing is claimed; a 504 may still land
      }
      throw r;
    }
    if (claimed) {
      claims.remove(key, who); // the backend shows the turn now, or it left nothing behind
    }
    answer(x, conversationId, result);
  }

  private static boolean sameOwner(Principal a, Principal b) {
    return a.subject().equals(b.subject()) && java.util.Objects.equals(a.tenant(), b.tenant());
  }

  private void resume(HttpExchange x, Principal who, String conversationId, String turnId)
      throws IOException, Refusal {
    Map<String, Object> signal = body(x);
    String key = scopedKey(who.tenant(), conversationId);
    List<LogEvent> events = await(backend.conversation(key));
    if (events.stream().noneMatch(e -> turnId.equals(e.turnId()))) {
      throw new Refusal(404, NOT_FOUND); // no such turn says no more than no such conversation
    }
    TurnResult result = await(backend.submit(new Event(key, turnId, who.subject(), "", Map.of(), signal, who)));
    Principal owner = ownerOf(events);
    boolean insider = owner != null && sameOwner(owner, who)
        || who.roles().stream().anyMatch(options.readerRoles()::contains);
    if (!insider && result.events.stream().anyMatch(e -> e.is(EventType.SIGNAL_REFUSED))) {
      // Someone who may not answer this turn learns that, and nothing about the conversation.
      Map<String, Object> out = new LinkedHashMap<>();
      out.put("conversation_id", conversationId);
      out.put("turn_id", turnId);
      out.put("status", result.status.wire());
      out.put("error", result.error == null ? null : result.error.toMap());
      send(x, 403, JSON.writeValueAsBytes(out));
      return;
    }
    answer(x, conversationId, result);
  }

  private void readConversation(HttpExchange x, Principal who, String conversationId)
      throws IOException, Refusal {
    String key = scopedKey(who.tenant(), conversationId);
    List<LogEvent> events = requireAccess(who, key, true);
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("conversation_id", conversationId);
    out.put("events", events.stream().map(LogEvent::toMap).toList());
    send(x, 200, JSON.writeValueAsBytes(out));
  }

  /** The owner, or a reader on a read. Anyone else, like a key with nothing on it, gets a 404. */
  private List<LogEvent> requireAccess(Principal who, String key, boolean reading) throws Refusal {
    List<LogEvent> events = await(backend.conversation(key));
    if (events.isEmpty()) {
      if (reading) {
        throw new Refusal(404, NOT_FOUND);
      }
      return events;
    }
    claims.remove(key); // the backend shows the conversation now, and its first turn says who owns it
    Principal owner = ownerOf(events);
    boolean owns = owner != null && sameOwner(owner, who);
    boolean reader = reading && who.roles().stream().anyMatch(options.readerRoles()::contains);
    if (!owns && !reader) {
      throw new Refusal(404, NOT_FOUND);
    }
    return events;
  }

  private static Principal ownerOf(List<LogEvent> events) {
    for (LogEvent e : events) {
      if (e.is(EventType.TURN_RECEIVED)) {
        return Principal.fromMap(e.payload().get("principal"));
      }
    }
    return null;
  }

  private void answer(HttpExchange x, String conversationId, TurnResult result) throws IOException {
    Map<String, Object> out = new LinkedHashMap<>(result.toMap());
    out.put("conversation_id", conversationId);
    boolean refused = result.events.stream().anyMatch(e -> e.is(EventType.SIGNAL_REFUSED));
    boolean limited = result.events.stream().anyMatch(e -> e.is(EventType.LIMIT_EXCEEDED));
    send(x, limited ? 429 : refused ? 403 : 200, JSON.writeValueAsBytes(out));
  }

  private <T> T await(java.util.concurrent.CompletionStage<T> stage) throws Refusal {
    try {
      return stage.toCompletableFuture().get(options.timeout().toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new Refusal(504, error("the runtime did not answer in time; redeliver the same turn_id"));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new Refusal(503, error("shutting down"));
    } catch (java.util.concurrent.ExecutionException e) {
      throw new Refusal(502, error("the runtime could not take the turn"));
    }
  }

  private static Map<String, Object> body(HttpExchange x) throws IOException, Refusal {
    try (InputStream in = x.getRequestBody()) {
      byte[] raw = in.readNBytes(MAX_BODY + 1);
      if (raw.length > MAX_BODY) {
        throw new Refusal(413, error("the body is too large"));
      }
      return JSON.readValue(raw, OBJECT);
    }
  }

  private static String required(Map<String, Object> body, String field) throws Refusal {
    if (body.get(field) instanceof String s && !s.isBlank()) {
      return s;
    }
    throw new Refusal(400, error(field + " is required"));
  }

  private static byte[] error(String message) {
    try {
      return JSON.writeValueAsBytes(Map.of("error", message));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void send(HttpExchange x, int status, byte[] body) throws IOException {
    x.getResponseHeaders().set("Content-Type", "application/json");
    x.sendResponseHeaders(status, body.length);
    x.getResponseBody().write(body);
  }

  /** An answer that is not a result. */
  private static final class Refusal extends Exception {
    private static final long serialVersionUID = 1L;
    final int status;
    final transient byte[] body;

    Refusal(int status, byte[] body) {
      super(null, null, false, false);
      this.status = status;
      this.body = body;
    }
  }
}
