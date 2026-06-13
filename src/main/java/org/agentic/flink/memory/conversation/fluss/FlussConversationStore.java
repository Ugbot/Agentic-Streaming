package org.agentic.flink.memory.conversation.fluss;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.agentic.flink.config.AgenticFlinkConfig;
import org.agentic.flink.config.ConfigKeys;
import org.agentic.flink.llm.ChatMessage;
import org.agentic.flink.memory.conversation.ConversationStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Fluss-backed {@link ConversationStore} — a PK-table "state spine" alternative to {@link
 * org.agentic.flink.memory.conversation.redis.RedisConversationStore}. The cross-operator,
 * cross-turn continuity the routed graph + the {@code A2AStep.applyToStateful} split need (remote
 * contextId, routing phase, transcript) lives in a single primary-keyed Fluss table, so it survives
 * across distinct operators, across turns, and across checkpoint/restore.
 *
 * <p>Layout — one PK table {@code (key STRING PK, payload STRING)}:
 *
 * <pre>
 *   key = {conversationId}              -&gt; JSON envelope {msgs:[...], attrs:{...}, owner:"..."}
 *   key = "__idx__:all"                   -&gt; JSON array of known conversationIds
 *   key = "__idx__:user:{userId}"    -&gt; JSON array of that user's conversationIds
 * </pre>
 *
 * Index keys use a printable "__idx__:" prefix that can never collide with a real conversationId. Each
 * mutation is a lookup → {@link FlussConversationCodec read-modify-write} → upsert; per-conversation
 * turns are sequential in the routed graph, so contention on a single envelope is not a concern.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}; the no-arg constructor self-gates on {@link
 * ConfigKeys#CONVERSATION_STORE}{@code =fluss} (throws otherwise so discovery falls back to the
 * in-JVM store). {@link java.io.Serializable} config fields ship in the job graph; the Fluss
 * connection/table/writer/lookuper + mapper are {@code transient} and built lazily on the task side.
 * Per the SPI contract these methods degrade gracefully (log + empty/no-op) rather than fail a turn.
 */
public final class FlussConversationStore implements ConversationStore {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(FlussConversationStore.class);

  private static final int COL_KEY = 0;
  private static final int COL_PAYLOAD = 1;
  private static final String ALL_KEY = "__idx__:all";
  private static final String USER_PREFIX = "__idx__:user:";

  private final String bootstrapServers;
  private final String database;
  private final String table;
  private final int buckets;
  private final int maxMessages;

  private transient volatile Connection connection;
  private transient volatile Table flussTable;
  private transient volatile UpsertWriter writer;
  private transient volatile Lookuper lookuper;
  private transient volatile ObjectMapper mapper;

  /** ServiceLoader constructor — configures from the environment; throws if Fluss is not selected. */
  public FlussConversationStore() {
    this(AgenticFlinkConfig.fromEnvironment(), true);
  }

  /** Explicit constructor (tests / programmatic wiring). Always enabled. */
  public FlussConversationStore(String bootstrapServers, String database, String table) {
    this.bootstrapServers = bootstrapServers;
    this.database = database;
    this.table = table;
    this.buckets = Integer.parseInt(ConfigKeys.DEFAULT_CONVERSATION_STORE_FLUSS_BUCKETS);
    this.maxMessages = Integer.parseInt(ConfigKeys.DEFAULT_CONVERSATION_STORE_MAX_MESSAGES);
  }

  private FlussConversationStore(AgenticFlinkConfig config, boolean requireSelected) {
    if (requireSelected) {
      String selected =
          config.get(ConfigKeys.CONVERSATION_STORE, ConfigKeys.DEFAULT_CONVERSATION_STORE);
      if (!"fluss".equalsIgnoreCase(selected)) {
        throw new IllegalStateException(
            "FlussConversationStore not selected (" + ConfigKeys.CONVERSATION_STORE + "=" + selected + ")");
      }
    }
    this.bootstrapServers =
        config.get(ConfigKeys.FLUSS_BOOTSTRAP_SERVERS, ConfigKeys.DEFAULT_FLUSS_BOOTSTRAP_SERVERS);
    this.database =
        config.get(
            ConfigKeys.CONVERSATION_STORE_FLUSS_DATABASE,
            ConfigKeys.DEFAULT_CONVERSATION_STORE_FLUSS_DATABASE);
    this.table =
        config.get(
            ConfigKeys.CONVERSATION_STORE_FLUSS_TABLE,
            ConfigKeys.DEFAULT_CONVERSATION_STORE_FLUSS_TABLE);
    this.buckets =
        config.getInt(
            ConfigKeys.CONVERSATION_STORE_FLUSS_BUCKETS,
            Integer.parseInt(ConfigKeys.DEFAULT_CONVERSATION_STORE_FLUSS_BUCKETS));
    this.maxMessages =
        config.getInt(
            ConfigKeys.CONVERSATION_STORE_MAX_MESSAGES,
            Integer.parseInt(ConfigKeys.DEFAULT_CONVERSATION_STORE_MAX_MESSAGES));
    LOG.info(
        "FlussConversationStore enabled: bootstrap={} db={} table={} buckets={} cap={}",
        bootstrapServers, database, table, buckets, maxMessages);
  }

  // ==================== transcript ====================

  @Override
  public void append(String conversationId, ChatMessage message) {
    if (conversationId == null || message == null) {
      return;
    }
    try {
      String env = readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE);
      writeRow(conversationId, FlussConversationCodec.appendMessage(mapper(), env, message, maxMessages));
      addToIndex(ALL_KEY, conversationId);
    } catch (Exception e) {
      LOG.warn("fluss append failed for {}: {}", conversationId, e.toString());
    }
  }

  @Override
  public List<ChatMessage> history(String conversationId) {
    if (conversationId == null) {
      return new ArrayList<>();
    }
    try {
      String env = readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE);
      return FlussConversationCodec.messages(mapper(), env);
    } catch (Exception e) {
      LOG.warn("fluss history failed for {}: {}", conversationId, e.toString());
      return new ArrayList<>();
    }
  }

  @Override
  public int messageCount(String conversationId) {
    if (conversationId == null) {
      return 0;
    }
    try {
      String env = readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE);
      return FlussConversationCodec.messageCount(mapper(), env);
    } catch (Exception e) {
      LOG.warn("fluss messageCount failed for {}: {}", conversationId, e.toString());
      return 0;
    }
  }

  // ==================== attributes ====================

  @Override
  public void putAttribute(String conversationId, String key, String value) {
    if (conversationId == null || key == null) {
      return;
    }
    try {
      String env = readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE);
      writeRow(conversationId, FlussConversationCodec.putAttribute(mapper(), env, key, value));
      addToIndex(ALL_KEY, conversationId);
    } catch (Exception e) {
      LOG.warn("fluss putAttribute failed for {}: {}", conversationId, e.toString());
    }
  }

  @Override
  public Optional<String> getAttribute(String conversationId, String key) {
    if (conversationId == null || key == null) {
      return Optional.empty();
    }
    try {
      String env = readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE);
      return FlussConversationCodec.getAttribute(mapper(), env, key);
    } catch (Exception e) {
      LOG.warn("fluss getAttribute failed for {}: {}", conversationId, e.toString());
      return Optional.empty();
    }
  }

  @Override
  public Map<String, String> attributes(String conversationId) {
    if (conversationId == null) {
      return new LinkedHashMap<>();
    }
    try {
      String env = readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE);
      return FlussConversationCodec.attributes(mapper(), env);
    } catch (Exception e) {
      LOG.warn("fluss attributes failed for {}: {}", conversationId, e.toString());
      return new LinkedHashMap<>();
    }
  }

  // ==================== user index ====================

  @Override
  public void associateUser(String conversationId, String userId) {
    if (conversationId == null || userId == null) {
      return;
    }
    try {
      String env = readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE);
      Optional<String> prior = FlussConversationCodec.owner(mapper(), env);
      if (prior.isPresent() && !prior.get().equals(userId)) {
        removeFromIndex(USER_PREFIX + prior.get(), conversationId);
      }
      writeRow(conversationId, FlussConversationCodec.setOwner(mapper(), env, userId));
      addToIndex(USER_PREFIX + userId, conversationId);
      addToIndex(ALL_KEY, conversationId);
    } catch (Exception e) {
      LOG.warn("fluss associateUser failed for {}: {}", conversationId, e.toString());
    }
  }

  @Override
  public Optional<String> userOf(String conversationId) {
    if (conversationId == null) {
      return Optional.empty();
    }
    try {
      String env = readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE);
      return FlussConversationCodec.owner(mapper(), env);
    } catch (Exception e) {
      LOG.warn("fluss userOf failed for {}: {}", conversationId, e.toString());
      return Optional.empty();
    }
  }

  @Override
  public List<String> conversationsForUser(String userId) {
    if (userId == null) {
      return new ArrayList<>();
    }
    try {
      String list = readRow(USER_PREFIX + userId).orElse(FlussConversationCodec.EMPTY_LIST);
      return FlussConversationCodec.decodeList(mapper(), list);
    } catch (Exception e) {
      LOG.warn("fluss conversationsForUser failed for {}: {}", userId, e.toString());
      return new ArrayList<>();
    }
  }

  @Override
  public void clear(String conversationId) {
    if (conversationId == null) {
      return;
    }
    try {
      Optional<String> owner =
          FlussConversationCodec.owner(
              mapper(), readRow(conversationId).orElse(FlussConversationCodec.EMPTY_ENVELOPE));
      // Tombstone with an empty envelope rather than a PK delete: a deleted PK row is not reliably
      // absent from a subsequent lookup, whereas an empty envelope makes history/attributes/owner all
      // read empty — contract-equivalent to "forgotten" — and the conversation is dropped from the
      // ALL/user indexes below so it no longer lists.
      writeRow(conversationId, FlussConversationCodec.EMPTY_ENVELOPE);
      if (owner.isPresent()) {
        removeFromIndex(USER_PREFIX + owner.get(), conversationId);
      }
      removeFromIndex(ALL_KEY, conversationId);
    } catch (Exception e) {
      LOG.warn("fluss clear failed for {}: {}", conversationId, e.toString());
    }
  }

  @Override
  public List<String> conversations() {
    try {
      String list = readRow(ALL_KEY).orElse(FlussConversationCodec.EMPTY_LIST);
      return FlussConversationCodec.decodeList(mapper(), list);
    } catch (Exception e) {
      LOG.warn("fluss conversations failed: {}", e.toString());
      return new ArrayList<>();
    }
  }

  // ==================== index helpers ====================

  private void addToIndex(String indexKey, String id) throws Exception {
    String list = readRow(indexKey).orElse(FlussConversationCodec.EMPTY_LIST);
    writeRow(indexKey, FlussConversationCodec.addToList(mapper(), list, id));
  }

  private void removeFromIndex(String indexKey, String id) throws Exception {
    String list = readRow(indexKey).orElse(FlussConversationCodec.EMPTY_LIST);
    writeRow(indexKey, FlussConversationCodec.removeFromList(mapper(), list, id));
  }

  // ==================== Fluss plumbing ====================

  private Optional<String> readRow(String key) throws Exception {
    GenericRow keyRow = new GenericRow(1);
    keyRow.setField(0, BinaryString.fromString(key));
    InternalRow row = lookuper().lookup(keyRow).get().getSingletonRow();
    if (row == null || row.isNullAt(COL_PAYLOAD)) {
      return Optional.empty();
    }
    return Optional.of(row.getString(COL_PAYLOAD).toString());
  }

  private void writeRow(String key, String payload) throws Exception {
    GenericRow row = new GenericRow(2);
    row.setField(COL_KEY, BinaryString.fromString(key));
    row.setField(COL_PAYLOAD, BinaryString.fromString(payload));
    UpsertWriter w = writer();
    w.upsert(row).get();
    w.flush();
    // Fluss PK lookups are not strictly read-your-writes the instant an upsert is acked (the kv apply
    // lags the log ack slightly). Our read-modify-write chains (transcript append, index updates)
    // depend on the next lookup seeing this write, so confirm visibility with a short bounded poll.
    awaitVisible(key, payload);
  }

  /** Poll the PK lookup until it reflects the just-written payload, bounded to ~2s. */
  private void awaitVisible(String key, String expectedPayload) throws Exception {
    for (int attempt = 0; attempt < 40; attempt++) {
      if (readRow(key).map(expectedPayload::equals).orElse(false)) {
        return;
      }
      Thread.sleep(50);
    }
    LOG.warn("fluss write to {} not visible within timeout; proceeding (eventual consistency)", key);
  }

  private void ensureReady() {
    if (flussTable != null) {
      return;
    }
    synchronized (this) {
      if (flussTable != null) {
        return;
      }
      try {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.BOOTSTRAP_SERVERS, Arrays.asList(bootstrapServers.split(",")));
        Connection conn = ConnectionFactory.createConnection(conf);
        TablePath path = TablePath.of(database, table);
        try (Admin admin = conn.getAdmin()) {
          if (!admin.databaseExists(database).get()) {
            admin.createDatabase(database, DatabaseDescriptor.EMPTY, true).get();
          }
          if (!admin.tableExists(path).get()) {
            Schema schema =
                Schema.newBuilder()
                    .column("key", DataTypes.STRING())
                    .column("payload", DataTypes.STRING())
                    .primaryKey("key")
                    .build();
            TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(buckets, "key").build();
            admin.createTable(path, descriptor, true).get();
            LOG.info("fluss conversation store created table {} (buckets={})", path, buckets);
          }
        }
        Table t = conn.getTable(path);
        this.connection = conn;
        this.writer = t.newUpsert().createWriter();
        this.lookuper = t.newLookup().createLookuper();
        this.flussTable = t; // published last: signals readiness
      } catch (Exception e) {
        throw new IllegalStateException(
            "FlussConversationStore: failed to connect to " + bootstrapServers, e);
      }
    }
  }

  private UpsertWriter writer() {
    ensureReady();
    return writer;
  }

  private Lookuper lookuper() {
    ensureReady();
    return lookuper;
  }

  private ObjectMapper mapper() {
    ObjectMapper m = mapper;
    if (m == null) {
      synchronized (this) {
        if (mapper == null) {
          mapper = new ObjectMapper();
        }
        m = mapper;
      }
    }
    return m;
  }
}
