package stores

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jagentic/goagentic/core"
)

// PostgresLongTermStore is a real core.LongTermStore backed by Postgres (pgx) —
// conversation resumption + a per-user fact archive that survives restarts.
type PostgresLongTermStore struct {
	pool   *pgxpool.Pool
	schema string
}

// NewPostgresLongTermStore connects and creates its tables.
func NewPostgresLongTermStore(url, schema string) (*PostgresLongTermStore, error) {
	if schema == "" {
		schema = "agentic"
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	stmts := []string{
		"CREATE SCHEMA IF NOT EXISTS " + schema,
		"CREATE TABLE IF NOT EXISTS " + schema + ".turns (id BIGSERIAL PRIMARY KEY, " +
			"conversation_id TEXT NOT NULL, user_id TEXT NOT NULL, role TEXT NOT NULL, " +
			"content TEXT NOT NULL, ts TIMESTAMPTZ DEFAULT now())",
		"CREATE TABLE IF NOT EXISTS " + schema + ".facts (user_id TEXT NOT NULL, key TEXT NOT NULL, " +
			"value TEXT NOT NULL, PRIMARY KEY (user_id, key))",
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			pool.Close()
			return nil, err
		}
	}
	return &PostgresLongTermStore{pool: pool, schema: schema}, nil
}

// Close releases the connection pool.
func (s *PostgresLongTermStore) Close() { s.pool.Close() }

func (s *PostgresLongTermStore) SaveTurn(conversationID, userID, role, content string) {
	_, _ = s.pool.Exec(context.Background(),
		"INSERT INTO "+s.schema+".turns (conversation_id, user_id, role, content) VALUES ($1,$2,$3,$4)",
		conversationID, userID, role, content)
}

func (s *PostgresLongTermStore) LoadHistory(conversationID string) [][2]string {
	rows, err := s.pool.Query(context.Background(),
		"SELECT role, content FROM "+s.schema+".turns WHERE conversation_id=$1 ORDER BY id", conversationID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var role, content string
		if rows.Scan(&role, &content) == nil {
			out = append(out, [2]string{role, content})
		}
	}
	return out
}

func (s *PostgresLongTermStore) SaveFact(userID, key, value string) {
	_, _ = s.pool.Exec(context.Background(),
		"INSERT INTO "+s.schema+".facts (user_id, key, value) VALUES ($1,$2,$3) "+
			"ON CONFLICT (user_id, key) DO UPDATE SET value=EXCLUDED.value", userID, key, value)
}

func (s *PostgresLongTermStore) Facts(userID string) map[string]string {
	out := map[string]string{}
	rows, err := s.pool.Query(context.Background(),
		"SELECT key, value FROM "+s.schema+".facts WHERE user_id=$1", userID)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) == nil {
			out[k] = v
		}
	}
	return out
}

func (s *PostgresLongTermStore) ConversationsForUser(userID string) []string {
	rows, err := s.pool.Query(context.Background(),
		"SELECT DISTINCT conversation_id FROM "+s.schema+".turns WHERE user_id=$1 ORDER BY conversation_id", userID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if rows.Scan(&c) == nil {
			out = append(out, c)
		}
	}
	return out
}

var _ core.LongTermStore = (*PostgresLongTermStore)(nil)
