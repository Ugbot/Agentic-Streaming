package org.jagentic.core.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jagentic.core.LongTermStore;

/** Real {@link LongTermStore} backed by Postgres (JDBC) — conversation resumption + a
 * per-user fact archive that survives restarts. Reference durable impl. */
public final class PostgresLongTermStore implements LongTermStore {

  private final String url, user, password, schema;

  public PostgresLongTermStore(String jdbcUrl, String user, String password, String schema) {
    this.url = jdbcUrl;
    this.user = user;
    this.password = password;
    this.schema = schema == null || schema.isBlank() ? "agentic" : schema;
    try (Connection c = open(); Statement st = c.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + this.schema);
      st.execute("CREATE TABLE IF NOT EXISTS " + this.schema + ".turns (id BIGSERIAL PRIMARY KEY, "
          + "conversation_id TEXT NOT NULL, user_id TEXT NOT NULL, role TEXT NOT NULL, "
          + "content TEXT NOT NULL, ts TIMESTAMPTZ DEFAULT now())");
      st.execute("CREATE TABLE IF NOT EXISTS " + this.schema + ".facts (user_id TEXT NOT NULL, "
          + "key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (user_id, key))");
    } catch (Exception e) {
      throw new RuntimeException("postgres init: " + e.getMessage(), e);
    }
  }

  private Connection open() throws Exception {
    return DriverManager.getConnection(url, user, password);
  }

  @Override
  public void saveTurn(String conversationId, String userId, String role, String content) {
    try (Connection c = open();
         PreparedStatement ps = c.prepareStatement("INSERT INTO " + schema
             + ".turns (conversation_id, user_id, role, content) VALUES (?,?,?,?)")) {
      ps.setString(1, conversationId);
      ps.setString(2, userId);
      ps.setString(3, role);
      ps.setString(4, content);
      ps.executeUpdate();
    } catch (Exception e) {
      throw new RuntimeException("saveTurn: " + e.getMessage(), e);
    }
  }

  @Override
  public List<String[]> loadHistory(String conversationId) {
    List<String[]> out = new ArrayList<>();
    try (Connection c = open();
         PreparedStatement ps = c.prepareStatement("SELECT role, content FROM " + schema
             + ".turns WHERE conversation_id=? ORDER BY id")) {
      ps.setString(1, conversationId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(new String[] {rs.getString(1), rs.getString(2)});
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("loadHistory: " + e.getMessage(), e);
    }
    return out;
  }

  @Override
  public void saveFact(String userId, String key, String value) {
    try (Connection c = open();
         PreparedStatement ps = c.prepareStatement("INSERT INTO " + schema
             + ".facts (user_id, key, value) VALUES (?,?,?) "
             + "ON CONFLICT (user_id, key) DO UPDATE SET value=EXCLUDED.value")) {
      ps.setString(1, userId);
      ps.setString(2, key);
      ps.setString(3, value);
      ps.executeUpdate();
    } catch (Exception e) {
      throw new RuntimeException("saveFact: " + e.getMessage(), e);
    }
  }

  @Override
  public Map<String, String> facts(String userId) {
    Map<String, String> out = new LinkedHashMap<>();
    try (Connection c = open();
         PreparedStatement ps = c.prepareStatement("SELECT key, value FROM " + schema + ".facts WHERE user_id=?")) {
      ps.setString(1, userId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.put(rs.getString(1), rs.getString(2));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("facts: " + e.getMessage(), e);
    }
    return out;
  }

  @Override
  public List<String> conversationsForUser(String userId) {
    List<String> out = new ArrayList<>();
    try (Connection c = open();
         PreparedStatement ps = c.prepareStatement("SELECT DISTINCT conversation_id FROM " + schema
             + ".turns WHERE user_id=? ORDER BY conversation_id")) {
      ps.setString(1, userId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(rs.getString(1));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("conversationsForUser: " + e.getMessage(), e);
    }
    return out;
  }
}
