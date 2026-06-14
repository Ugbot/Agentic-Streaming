package stores

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jagentic/goagentic/core"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func TestQdrantVectorStoreAsColdTierIfAvailable(t *testing.T) {
	url := envOr("AGENTIC_TEST_QDRANT_URL", "http://localhost:6333")
	if _, err := (&http.Client{Timeout: 2 * time.Second}).Get(url + "/healthz"); err != nil {
		t.Skipf("Qdrant not reachable: %v", err)
	}
	embed := core.NewHashingEmbedder(256)
	vs, err := NewQdrantVectorStore(url, "agentic_go_test", 256)
	if err != nil {
		t.Skipf("qdrant build failed: %v", err)
	}
	for id, text := range core.BankingKB {
		vs.Upsert(id, embed.Embed(text), text)
	}
	hits := vs.Search(embed.Embed("how do I dispute a charge"), 2)
	if len(hits) == 0 || hits[0].ID != "kb_payments_dispute" {
		t.Fatalf("top hit = %+v, want kb_payments_dispute", hits)
	}
	// as the cold tier of a two-tier retriever (empty hot → cold provides the hit)
	retr := core.NewTwoTierRetriever(core.NewInMemoryHotVectorIndex(), vs.ColdSearch(), 4, 4)
	merged := retr.Retrieve(embed.Embed("how do I dispute a charge"), 2)
	if len(merged) == 0 || merged[0].ID != "kb_payments_dispute" {
		t.Fatalf("cold-tier retrieve = %+v", merged)
	}
}

func TestPostgresLongTermStoreIfAvailable(t *testing.T) {
	url := envOr("AGENTIC_TEST_PG_URL", "postgresql://agentic:agentic@localhost:5434/agentic")
	store, err := NewPostgresLongTermStore(url, "agentic")
	if err != nil {
		t.Skipf("Postgres not reachable: %v", err)
	}
	defer store.Close()
	cid := "c-" + randSuffix()
	uid := "u-" + randSuffix()
	store.SaveTurn(cid, uid, "user", "what is my balance?")
	store.SaveTurn(cid, uid, "assistant", "1234.56")
	hist := store.LoadHistory(cid)
	if len(hist) != 2 || hist[0][1] != "what is my balance?" {
		t.Fatalf("history = %v", hist)
	}
	store.SaveFact(uid, "tier", "gold")
	if store.Facts(uid)["tier"] != "gold" {
		t.Fatalf("facts = %v", store.Facts(uid))
	}
	if !contains(store.ConversationsForUser(uid), cid) {
		t.Fatalf("conversations missing %s", cid)
	}
}

func TestRedisConversationStoreIfAvailable(t *testing.T) {
	url := envOr("AGENTIC_TEST_REDIS_URL", "redis://localhost:6380/0")
	store, err := NewRedisConversationStore(url, 200)
	if err != nil {
		t.Skipf("Redis/Valkey not reachable: %v", err)
	}
	cid := "c-" + randSuffix()
	store.Append(cid, core.UserMessage("hi"))
	store.Append(cid, core.AssistantMessage("hello"))
	if store.MessageCount(cid) != 2 {
		t.Fatalf("count = %d", store.MessageCount(cid))
	}
	store.PutAttribute(cid, core.PathAttr, "cards")
	if v, ok := store.GetAttribute(cid, core.PathAttr); !ok || v != "cards" {
		t.Fatalf("attr = %v %v", v, ok)
	}
	store.AssociateUser(cid, "alice")
	if !contains(store.ConversationsForUser("alice"), cid) {
		t.Fatalf("user index missing %s", cid)
	}
	store.Clear(cid)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func randSuffix() string {
	return time.Now().Format("150405.000000")
}
