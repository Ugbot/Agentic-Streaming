package org.jagentic.ports.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.jagentic.core.Banking;
import org.jagentic.core.ChatMessage;

/**
 * Integration test: deploys {@link BankingFunction} to a real Apache Pulsar standalone
 * (the {@code apachepulsar/pulsar} image, run with Podman) and drives banking turns
 * through the input topic, asserting replies on the output topic and the transcript
 * persisted in Pulsar's BookKeeper-backed function state store.
 *
 * <p>Service discovery, in order:
 * <ol>
 *   <li>{@code AGENTIC_PULSAR_SERVICE_URL} and {@code AGENTIC_PULSAR_ADMIN_URL} point at an
 *       already running standalone with a functions worker and state storage enabled.</li>
 *   <li>Otherwise {@code podman} on the PATH starts {@code apachepulsar/pulsar:<version>}
 *       standalone (ZooKeeper mode, which is what enables the BookKeeper state store) on
 *       ports 6650 and 8080 and removes it afterwards.</li>
 *   <li>Otherwise the test is skipped and prints why. Any other failure, including a
 *       container that does not become healthy, is a test failure, never a pass.</li>
 * </ol>
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BankingFunctionStandaloneTest {

  private static final String IMAGE = System.getProperty("agentic.pulsar.image",
      "docker.io/apachepulsar/pulsar:3.3.1");
  private static final String TENANT = "public";
  private static final String NAMESPACE = "default";
  private static final Duration BOOT_TIMEOUT = Duration.ofMinutes(6);
  private static final Duration FUNCTION_TIMEOUT = Duration.ofMinutes(3);
  private static final Duration REPLY_TIMEOUT = Duration.ofSeconds(90);

  /** Banking utterances with the path the shared core router assigns to each. */
  private static final String[][] TURNS = {
      {"what is my balance?", "payments"},
      {"what card types do you offer?", "cards"},
      {"tell me about crypto cash-back", "cards"},
      {"I want to dispute a charge", "payments"},
      {"what are your opening hours?", "general"},
      {"can I raise my transfer limit?", "payments"},
  };

  private final HttpClient http = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5)).build();

  private String skipReason;
  private String serviceUrl;
  private String adminUrl;
  private String containerName;
  private String functionName;
  private String inputTopic;
  private String outputTopic;
  private PulsarClient client;

  @BeforeAll
  void startPulsar() throws Exception {
    String envService = System.getenv("AGENTIC_PULSAR_SERVICE_URL");
    String envAdmin = System.getenv("AGENTIC_PULSAR_ADMIN_URL");
    if (envService != null && envAdmin != null) {
      serviceUrl = envService;
      adminUrl = envAdmin.replaceAll("/+$", "");
      System.out.println("[integration] using external Pulsar at " + serviceUrl + " / " + adminUrl);
    } else if (commandExists("podman")) {
      containerName = "agentic-pulsar-it-" + UUID.randomUUID().toString().substring(0, 8);
      System.out.println("[integration] starting " + IMAGE + " standalone as " + containerName);
      run(BOOT_TIMEOUT, "podman", "run", "-d", "--name", containerName,
          "-p", "6650:6650", "-p", "8080:8080",
          "-e", "PULSAR_STANDALONE_USE_ZOOKEEPER=1",
          IMAGE, "bin/pulsar", "standalone");
      serviceUrl = "pulsar://localhost:6650";
      adminUrl = "http://localhost:8080";
    } else {
      skipReason = "SKIPPED " + getClass().getSimpleName()
          + ": podman is not on the PATH and AGENTIC_PULSAR_SERVICE_URL/AGENTIC_PULSAR_ADMIN_URL"
          + " are unset, so there is no Pulsar standalone to deploy the function to.";
      System.out.println(skipReason);
      return;
    }

    awaitHttp(adminUrl + "/admin/v2/clusters", BOOT_TIMEOUT, "broker admin API");
    awaitHttp(adminUrl + "/admin/v3/functions/" + TENANT + "/" + NAMESPACE, BOOT_TIMEOUT,
        "functions worker");

    String suffix = UUID.randomUUID().toString().substring(0, 8);
    functionName = "banking-" + suffix;
    inputTopic = "banking-requests-" + suffix;
    outputTopic = "banking-responses-" + suffix;

    Path jar = Files.createTempFile("agentic-pulsar-function", ".jar");
    buildFunctionJar(jar);
    deployFunction(jar);
    awaitFunctionRunning();

    client = PulsarClient.builder().serviceUrl(serviceUrl).build();
  }

  @AfterAll
  void stopPulsar() throws Exception {
    if (client != null) {
      client.close();
    }
    if (functionName != null && adminUrl != null) {
      try {
        send(HttpRequest.newBuilder(URI.create(functionUrl())).DELETE().build());
      } catch (RuntimeException e) {
        System.out.println("[integration] function cleanup failed: " + e.getMessage());
      }
    }
    if (containerName != null) {
      run(Duration.ofMinutes(2), "podman", "rm", "-f", containerName);
    }
  }

  @Test
  void deployedFunctionRoutesTurnsAndPersistsTranscriptsInPulsarState() throws Exception {
    Assumptions.assumeTrue(skipReason == null, () -> skipReason);
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    Map<String, String> userOf = new LinkedHashMap<>();
    Map<String, List<String[]>> turnsOf = new LinkedHashMap<>();
    int total = 0;
    for (int c = 0; c < 3; c++) {
      String cid = "conv-" + UUID.randomUUID();
      String userId = "user-" + rnd.nextInt(1_000_000);
      List<String[]> turns = new ArrayList<>();
      int n = 1 + rnd.nextInt(3);
      for (int t = 0; t < n; t++) {
        turns.add(TURNS[rnd.nextInt(TURNS.length)]);
      }
      userOf.put(cid, userId);
      turnsOf.put(cid, turns);
      total += n;
    }

    try (Consumer<String> replies = client.newConsumer(Schema.STRING)
            .topic(outputTopic)
            .subscriptionName("it-" + UUID.randomUUID())
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();
         Producer<String> requests = client.newProducer(Schema.STRING).topic(inputTopic).create()) {

      for (Map.Entry<String, List<String[]>> e : turnsOf.entrySet()) {
        for (String[] turn : e.getValue()) {
          requests.newMessage().key(e.getKey()).property("userId", userOf.get(e.getKey()))
              .value(turn[0]).send();
        }
      }

      Map<String, List<Message<String>>> received = receive(replies, total);
      for (Map.Entry<String, List<String[]>> e : turnsOf.entrySet()) {
        String cid = e.getKey();
        List<Message<String>> got = received.getOrDefault(cid, List.of());
        assertEquals(e.getValue().size(), got.size(), "replies for " + cid);
        for (int i = 0; i < got.size(); i++) {
          String expectedPath = e.getValue().get(i)[1];
          assertTrue(got.get(i).getValue().startsWith("[" + expectedPath + "]"),
              "turn " + i + " of " + cid + " expected path " + expectedPath + " but reply was "
                  + got.get(i).getValue());
          assertEquals(userOf.get(cid), got.get(i).getProperty("userId"),
              "userId property forwarded to the output topic");
        }
      }

      for (String cid : turnsOf.keySet()) {
        PulsarStateConversationStore store = stateSnapshot(cid, userOf.get(cid));
        List<ChatMessage> history = store.history(cid);
        int turns = turnsOf.get(cid).size();
        assertEquals(2 * turns, history.size(), "user + assistant message per turn for " + cid);
        for (int i = 0; i < turns; i++) {
          assertEquals("user", history.get(2 * i).role());
          assertEquals(turnsOf.get(cid).get(i)[0], history.get(2 * i).content());
          assertEquals("assistant", history.get(2 * i + 1).role());
        }
        assertEquals(List.of(cid), store.conversationsForUser(userOf.get(cid)));
      }

      // Restart the function instance: state must come back from BookKeeper, not the heap.
      send(HttpRequest.newBuilder(URI.create(functionUrl() + "/restart"))
          .POST(HttpRequest.BodyPublishers.noBody()).build());
      awaitFunctionRunning();

      String cid = turnsOf.keySet().iterator().next();
      String[] turn = TURNS[rnd.nextInt(TURNS.length)];
      requests.newMessage().key(cid).property("userId", userOf.get(cid)).value(turn[0]).send();
      Message<String> after = received(replies, cid);
      assertTrue(after.getValue().startsWith("[" + turn[1] + "]"), after.getValue());
      PulsarStateConversationStore store = stateSnapshot(cid, userOf.get(cid));
      assertEquals(2 * (turnsOf.get(cid).size() + 1), store.messageCount(cid),
          "transcript survived the function instance restart");
    }
  }

  // --- Pulsar state readback -----------------------------------------------------------

  /** Reads the conversation and user index entries back over the admin API. */
  private PulsarStateConversationStore stateSnapshot(String cid, String userId) {
    Map<String, byte[]> values = new HashMap<>();
    values.put("conv/" + cid, stateValue("conv/" + cid));
    values.put("user/" + userId, stateValue("user/" + userId));
    StateBytes bytes = new StateBytes() {
      @Override public byte[] get(String key) { return values.get(key); }
      @Override public void put(String key, byte[] value) { values.put(key, value); }
      @Override public void delete(String key) { values.remove(key); }
    };
    return new PulsarStateConversationStore(bytes);
  }

  private byte[] stateValue(String key) {
    String encoded = URLEncoder.encode(key, StandardCharsets.UTF_8);
    String body = send(HttpRequest.newBuilder(URI.create(functionUrl() + "/state/" + encoded))
        .GET().build());
    Matcher m = Pattern.compile("\"byteValue\"\\s*:\\s*\"([A-Za-z0-9+/=]+)\"").matcher(body);
    assertTrue(m.find(), "state entry " + key + " has a byteValue: " + body);
    return Base64.getDecoder().decode(m.group(1));
  }

  // --- messaging helpers ---------------------------------------------------------------

  private Map<String, List<Message<String>>> receive(Consumer<String> consumer, int count)
      throws Exception {
    Map<String, List<Message<String>>> byKey = new HashMap<>();
    long deadline = System.nanoTime() + REPLY_TIMEOUT.toNanos();
    int seen = 0;
    while (seen < count) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        fail("expected " + count + " replies on " + outputTopic + " but received " + seen);
      }
      Message<String> msg = consumer.receive((int) Math.min(5_000, remaining / 1_000_000), TimeUnit.MILLISECONDS);
      if (msg == null) {
        continue;
      }
      consumer.acknowledge(msg);
      byKey.computeIfAbsent(msg.getKey(), k -> new ArrayList<>()).add(msg);
      seen++;
    }
    return byKey;
  }

  private Message<String> received(Consumer<String> consumer, String key) throws Exception {
    long deadline = System.nanoTime() + REPLY_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
      if (msg == null) {
        continue;
      }
      consumer.acknowledge(msg);
      if (key.equals(msg.getKey())) {
        return msg;
      }
    }
    throw new AssertionError("no reply for " + key + " within " + REPLY_TIMEOUT);
  }

  // --- function deployment -------------------------------------------------------------

  private String functionUrl() {
    return adminUrl + "/admin/v3/functions/" + TENANT + "/" + NAMESPACE + "/" + functionName;
  }

  /** The function jar is the adapter's compiled classes plus jagentic-core, as a deployed
   * function sees them; the Pulsar Functions API itself is provided by the worker. */
  private static void buildFunctionJar(Path target) throws IOException {
    Path adapterClasses = codeSource(BankingFunction.class);
    Path coreJar = codeSource(Banking.class);
    try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(target))) {
      try (Stream<Path> files = Files.walk(adapterClasses)) {
        for (Path p : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
          String name = adapterClasses.relativize(p).toString().replace('\\', '/');
          out.putNextEntry(new JarEntry(name));
          Files.copy(p, out);
          out.closeEntry();
        }
      }
      try (JarFile core = new JarFile(coreJar.toFile())) {
        for (JarEntry e : (Iterable<JarEntry>) core.stream()::iterator) {
          if (e.isDirectory() || e.getName().startsWith("META-INF/")) {
            continue;
          }
          out.putNextEntry(new JarEntry(e.getName()));
          try (InputStream in = core.getInputStream(e)) {
            in.transferTo(out);
          }
          out.closeEntry();
        }
      }
    }
  }

  private static Path codeSource(Class<?> type) {
    CodeSource source = type.getProtectionDomain().getCodeSource();
    assertNotNull(source, "code source of " + type);
    try {
      return Path.of(source.getLocation().toURI());
    } catch (java.net.URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  private void deployFunction(Path jar) throws IOException, InterruptedException {
    String config = "{"
        + "\"tenant\":\"" + TENANT + "\","
        + "\"namespace\":\"" + NAMESPACE + "\","
        + "\"name\":\"" + functionName + "\","
        + "\"className\":\"" + BankingFunction.class.getName() + "\","
        + "\"inputs\":[\"" + inputTopic + "\"],"
        + "\"output\":\"" + outputTopic + "\","
        + "\"parallelism\":1,"
        + "\"retainKeyOrdering\":true,"
        + "\"jar\":\"" + jar.getFileName() + "\""
        + "}";
    String boundary = "----agentic" + UUID.randomUUID();
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeAscii(body, "--" + boundary + "\r\n"
        + "Content-Disposition: form-data; name=\"functionConfig\"\r\n"
        + "Content-Type: application/json\r\n\r\n" + config + "\r\n");
    writeAscii(body, "--" + boundary + "\r\n"
        + "Content-Disposition: form-data; name=\"data\"; filename=\"" + jar.getFileName() + "\"\r\n"
        + "Content-Type: application/octet-stream\r\n\r\n");
    body.write(Files.readAllBytes(jar));
    writeAscii(body, "\r\n--" + boundary + "--\r\n");

    HttpRequest request = HttpRequest.newBuilder(URI.create(functionUrl()))
        .header("Content-Type", "multipart/form-data; boundary=" + boundary)
        .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
        .build();
    // The worker answers "Leader not yet ready" for a short while after it comes up.
    long deadline = System.nanoTime() + FUNCTION_TIMEOUT.toNanos();
    String lastError = "";
    while (System.nanoTime() < deadline) {
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() / 100 == 2) {
        System.out.println("[integration] deployed " + functionName + " from " + jar);
        return;
      }
      lastError = response.statusCode() + " " + response.body();
      Thread.sleep(2_000);
    }
    fail("could not deploy " + functionName + ": " + lastError);
  }

  private void awaitFunctionRunning() throws InterruptedException {
    long deadline = System.nanoTime() + FUNCTION_TIMEOUT.toNanos();
    String last = "";
    while (System.nanoTime() < deadline) {
      HttpResponse<String> response = trySend(functionUrl() + "/status");
      if (response != null && response.statusCode() == 200) {
        last = response.body();
        if (last.contains("\"numRunning\" : 1") || last.contains("\"numRunning\":1")) {
          return;
        }
      }
      Thread.sleep(2_000);
    }
    fail("function " + functionName + " did not reach numRunning=1: " + last);
  }

  // --- process and HTTP helpers --------------------------------------------------------

  private static void writeAscii(ByteArrayOutputStream out, String s) {
    byte[] b = s.getBytes(StandardCharsets.US_ASCII);
    out.write(b, 0, b.length);
  }

  private static boolean commandExists(String name) {
    String path = System.getenv("PATH");
    if (path == null) {
      return false;
    }
    for (String dir : path.split(java.io.File.pathSeparator)) {
      if (!dir.isEmpty() && Files.isExecutable(Path.of(dir, name))) {
        return true;
      }
    }
    return false;
  }

  private static void run(Duration timeout, String... command) throws IOException, InterruptedException {
    Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
    if (!p.waitFor(timeout.toSeconds(), TimeUnit.SECONDS)) {
      p.destroyForcibly();
      fail(String.join(" ", command) + " timed out after " + timeout);
    }
    if (p.exitValue() != 0) {
      fail(String.join(" ", command) + " exited " + p.exitValue() + ": " + output);
    }
  }

  private void awaitHttp(String url, Duration timeout, String what) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    String last = "no response";
    while (System.nanoTime() < deadline) {
      HttpResponse<String> r = trySend(url);
      if (r != null && r.statusCode() == 200) {
        System.out.println("[integration] " + what + " ready at " + url);
        return;
      }
      last = r == null ? last : r.statusCode() + " " + r.body();
      Thread.sleep(3_000);
    }
    fail(what + " at " + url + " not ready after " + timeout + " (last: " + last + ")");
  }

  private HttpResponse<String> trySend(String url) {
    try {
      return http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private String send(HttpRequest request) {
    try {
      HttpResponse<String> r = http.send(request, HttpResponse.BodyHandlers.ofString());
      if (r.statusCode() / 100 != 2) {
        throw new IllegalStateException(request.method() + " " + request.uri() + " -> "
            + r.statusCode() + " " + r.body());
      }
      return r.body();
    } catch (IOException e) {
      throw new IllegalStateException(request.method() + " " + request.uri(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
