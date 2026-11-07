package org.jagentic.core.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.jagentic.core.Retrieval;
import org.jagentic.core.VectorStore;

/** Real {@link VectorStore} backed by Postgres with the pgvector extension, for a cold tier that
 * lives in a database you already run. Cosine distance over an HNSW index, which is valid on an
 * empty table. The driver is the same optional dependency as {@link PostgresLongTermStore}. */
public final class PgVectorStore implements VectorStore {

  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private final String url, user, password, table;
  private final int dim;

  public PgVectorStore(String jdbcUrl, String user, String password, String table, int dim) {
    this.url = jdbcUrl;
    this.user = user;
    this.password = password;
    this.table = table == null || table.isBlank() ? "agentic_vectors" : table;
    this.dim = dim;
    if (!IDENTIFIER.matcher(this.table).matches()) {
      throw new IllegalArgumentException("pgvector table is not a plain identifier: " + this.table);
    }
    try {
      // DriverManager registers drivers from the system class loader, which on a Flink cluster
      // cannot see the job jar.
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("pgvector needs org.postgresql:postgresql on the classpath", e);
    }
    try (Connection c = open(); Statement st = c.createStatement()) {
      st.execute("CREATE EXTENSION IF NOT EXISTS vector");
      st.execute("CREATE TABLE IF NOT EXISTS " + this.table + " (id TEXT PRIMARY KEY, "
          + "embedding vector(" + dim + ") NOT NULL, text TEXT NOT NULL)");
      st.execute("CREATE INDEX IF NOT EXISTS " + this.table + "_embedding_idx ON " + this.table
          + " USING hnsw (embedding vector_cosine_ops)");
    } catch (Exception e) {
      throw new RuntimeException("pgvector init: " + e.getMessage(), e);
    }
  }

  private Connection open() throws Exception {
    return DriverManager.getConnection(url, user, password);
  }

  @Override
  public void upsert(String docId, float[] embedding, String text) {
    if (embedding.length != dim) {
      throw new IllegalArgumentException("embedding has " + embedding.length + " dimensions, not " + dim);
    }
    try (Connection c = open();
         PreparedStatement ps = c.prepareStatement("INSERT INTO " + table
             + " (id, embedding, text) VALUES (?, ?::vector, ?) ON CONFLICT (id) DO UPDATE"
             + " SET embedding = EXCLUDED.embedding, text = EXCLUDED.text")) {
      ps.setString(1, docId);
      ps.setString(2, literal(embedding));
      ps.setString(3, text);
      ps.executeUpdate();
    } catch (Exception e) {
      throw new RuntimeException("pgvector upsert: " + e.getMessage(), e);
    }
  }

  @Override
  public List<Retrieval.Scored> search(float[] query, int k) {
    List<Retrieval.Scored> out = new ArrayList<>();
    try (Connection c = open();
         PreparedStatement ps = c.prepareStatement("SELECT id, text, 1 - (embedding <=> ?::vector)"
             + " AS score FROM " + table + " ORDER BY embedding <=> ?::vector LIMIT ?")) {
      String vec = literal(query);
      ps.setString(1, vec);
      ps.setString(2, vec);
      ps.setInt(3, k);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(new Retrieval.Scored(rs.getString("id"), rs.getDouble("score"), rs.getString("text")));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("pgvector search: " + e.getMessage(), e);
    }
    return out;
  }

  private static String literal(float[] v) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < v.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(v[i]);
    }
    return sb.append(']').toString();
  }
}
