package stores

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/jagentic/goagentic/core"
	"github.com/redis/go-redis/v9"
)

// RedisConversationStore is a real core.ConversationStore backed by Redis/Valkey, using
// the same key scheme as the Python RedisConversationStore (transcript list + attrs hash
// + per-user set) so the two impls interoperate.
type RedisConversationStore struct {
	c      *redis.Client
	max    int
	prefix string
	ctx    context.Context
}

// NewRedisConversationStore connects via a redis:// URL (e.g. redis://localhost:6379/0).
func NewRedisConversationStore(url string, maxMessages int) (*RedisConversationStore, error) {
	if maxMessages <= 0 {
		maxMessages = 200
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(opts)
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &RedisConversationStore{c: c, max: maxMessages, prefix: "agentic", ctx: ctx}, nil
}

func (s *RedisConversationStore) msgs(cid string) string  { return s.prefix + ":conv:" + cid + ":msgs" }
func (s *RedisConversationStore) attrs(cid string) string { return s.prefix + ":conv:" + cid + ":attrs" }
func (s *RedisConversationStore) user(uid string) string  { return s.prefix + ":user:" + uid + ":convs" }

func (s *RedisConversationStore) Append(cid string, m core.ChatMessage) {
	b, _ := json.Marshal(map[string]string{"role": m.Role, "content": m.Content,
		"tool_name": m.ToolName, "tool_call_id": m.ToolCallID})
	key := s.msgs(cid)
	s.c.RPush(s.ctx, key, b)
	s.c.LTrim(s.ctx, key, int64(-s.max), -1)
}

func (s *RedisConversationStore) History(cid string) []core.ChatMessage {
	vals, err := s.c.LRange(s.ctx, s.msgs(cid), 0, -1).Result()
	if err != nil {
		return nil
	}
	out := make([]core.ChatMessage, 0, len(vals))
	for _, v := range vals {
		var d map[string]string
		if json.Unmarshal([]byte(v), &d) == nil {
			out = append(out, core.ChatMessage{Role: d["role"], Content: d["content"],
				ToolName: d["tool_name"], ToolCallID: d["tool_call_id"]})
		}
	}
	return out
}

func (s *RedisConversationStore) MessageCount(cid string) int {
	return int(s.c.LLen(s.ctx, s.msgs(cid)).Val())
}

func (s *RedisConversationStore) PutAttribute(cid, key, value string) {
	s.c.HSet(s.ctx, s.attrs(cid), key, value)
}

func (s *RedisConversationStore) GetAttribute(cid, key string) (string, bool) {
	v, err := s.c.HGet(s.ctx, s.attrs(cid), key).Result()
	if err != nil {
		return "", false
	}
	return v, true
}

func (s *RedisConversationStore) Attributes(cid string) map[string]string {
	all, _ := s.c.HGetAll(s.ctx, s.attrs(cid)).Result()
	out := map[string]string{}
	for k, v := range all {
		if len(k) < 2 || k[:2] != "__" {
			out[k] = v
		}
	}
	return out
}

func (s *RedisConversationStore) AssociateUser(cid, userID string) {
	prior, _ := s.c.HGet(s.ctx, s.attrs(cid), "__owner__").Result()
	if prior != "" && prior != userID {
		s.c.SRem(s.ctx, s.user(prior), cid)
	}
	s.c.HSet(s.ctx, s.attrs(cid), "__owner__", userID)
	s.c.SAdd(s.ctx, s.user(userID), cid)
}

func (s *RedisConversationStore) ConversationsForUser(userID string) []string {
	out, _ := s.c.SMembers(s.ctx, s.user(userID)).Result()
	sort.Strings(out)
	return out
}

func (s *RedisConversationStore) Clear(cid string) {
	owner, _ := s.c.HGet(s.ctx, s.attrs(cid), "__owner__").Result()
	if owner != "" {
		s.c.SRem(s.ctx, s.user(owner), cid)
	}
	s.c.Del(s.ctx, s.msgs(cid), s.attrs(cid))
}

var _ core.ConversationStore = (*RedisConversationStore)(nil)
