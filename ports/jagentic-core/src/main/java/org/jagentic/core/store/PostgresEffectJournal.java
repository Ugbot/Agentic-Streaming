package org.jagentic.core.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import org.jagentic.core.EffectJournal;
import org.jagentic.core.EffectJournals;

/**
 * The journal as one table, keyed by the call. An upsert is the whole write, and autocommit makes it
 * durable before {@link #put} returns. Rows are never removed here: retention is the operator's, and
 * must outlast the longest rollback or redelivery a deployment allows.
 */
public final class PostgresEffectJournal implements EffectJournal {
  private static final String TABLE = "agentic_effect_journal";

  private final String url;
  private final String user;
  private final String password;
  private Connection connection;

  public PostgresEffectJournal(String url, String user, String password) {
    this.url = url;
    this.user = user;
    this.password = password;
    try (Statement st = connection().createStatement()) {
      st.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (conversation_id TEXT NOT NULL, "
          + "turn_id TEXT NOT NULL, kind TEXT NOT NULL, idx INTEGER NOT NULL, entry TEXT NOT NULL, "
          + "updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
          + "PRIMARY KEY (conversation_id, turn_id, kind, idx))");
    } catch (SQLException e) {
      throw new Unavailable("cannot open the effect journal at " + url, e);
    }
  }

  private synchronized Connection connection() throws SQLException {
    if (connection == null || connection.isClosed()) {
      connection = DriverManager.getConnection(url, user, password);
    }
    return connection;
  }

  private static void bind(PreparedStatement st, Key key) throws SQLException {
    st.setString(1, key.conversationId());
    st.setString(2, key.turnId());
    st.setString(3, key.kind());
    st.setInt(4, key.index());
  }

  @Override
  public synchronized Optional<Entry> get(Key key) {
    String sql = "SELECT entry FROM " + TABLE
        + " WHERE conversation_id = ? AND turn_id = ? AND kind = ? AND idx = ?";
    try (PreparedStatement st = connection().prepareStatement(sql)) {
      bind(st, key);
      try (ResultSet rows = st.executeQuery()) {
        return rows.next() ? Optional.of(EffectJournals.fromJson(rows.getString(1))) : Optional.empty();
      }
    } catch (SQLException e) {
      connection = null;
      throw new Unavailable("cannot read the effect journal", e);
    }
  }

  @Override
  public synchronized void put(Entry entry) {
    String sql = "INSERT INTO " + TABLE + " (conversation_id, turn_id, kind, idx, entry) VALUES (?, ?, ?, ?, ?) "
        + "ON CONFLICT (conversation_id, turn_id, kind, idx) DO UPDATE SET entry = EXCLUDED.entry, updated_at = now()";
    write(sql, entry);
  }

  @Override
  public synchronized Optional<Entry> putIfAbsent(Entry entry) {
    String sql = "INSERT INTO " + TABLE + " (conversation_id, turn_id, kind, idx, entry) VALUES (?, ?, ?, ?, ?) "
        + "ON CONFLICT (conversation_id, turn_id, kind, idx) DO NOTHING";
    return write(sql, entry) == 1 ? Optional.empty() : get(entry.key());
  }

  private int write(String sql, Entry entry) {
    try (PreparedStatement st = connection().prepareStatement(sql)) {
      bind(st, entry.key());
      st.setString(5, EffectJournals.toJson(entry));
      return st.executeUpdate();
    } catch (SQLException e) {
      connection = null;
      throw new Unavailable("cannot write the effect journal", e);
    }
  }

  @Override
  public synchronized void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      throw new Unavailable("cannot close the effect journal", e);
    } finally {
      connection = null;
    }
  }
}
