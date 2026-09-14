package core

import "testing"

func TestSlidingWindowCount(t *testing.T) {
	w := NewSlidingWindow(60000)
	var last WindowState
	for ts := int64(1000); ts <= 5000; ts += 1000 {
		last = w.AddCount("h1", ts)
	}
	if last.Count != 5 {
		t.Fatalf("sliding count: want 5, got %d", last.Count)
	}
	// Far in the future — all prior entries evicted, only this one remains.
	if got := w.AddCount("h1", 200000); got.Count != 1 {
		t.Fatalf("sliding count after gap: want 1, got %d", got.Count)
	}
	// Independent per key.
	if got := w.AddCount("h2", 1000); got.Count != 1 {
		t.Fatalf("sliding count h2: want 1, got %d", got.Count)
	}
}

func TestSlidingWindowSum(t *testing.T) {
	w := NewSlidingWindow(10000)
	w.Add("acct", 0, 100)
	got := w.Add("acct", 5000, 250)
	if got.Count != 2 {
		t.Fatalf("sliding sum count: want 2, got %d", got.Count)
	}
	if got.Sum != 350 {
		t.Fatalf("sliding sum: want 350, got %v", got.Sum)
	}
}

func TestTumblingWindow(t *testing.T) {
	w := NewTumblingWindow(1000)
	if _, ok := w.AddCount("k", 100); ok {
		t.Fatalf("tumbling @100: want ok=false")
	}
	if _, ok := w.AddCount("k", 200); ok {
		t.Fatalf("tumbling @200: want ok=false")
	}
	b, ok := w.AddCount("k", 1500)
	if !ok {
		t.Fatalf("tumbling @1500: want ok=true")
	}
	if b.Start != 0 || b.Count != 2 {
		t.Fatalf("tumbling emitted bucket: want {Start:0, Count:2}, got %+v", b)
	}
	cb, ok := w.Close("k")
	if !ok {
		t.Fatalf("tumbling close: want ok=true")
	}
	if cb.Start != 1000 || cb.Count != 1 {
		t.Fatalf("tumbling close bucket: want {Start:1000, Count:1}, got %+v", cb)
	}
}

func TestSessionWindow(t *testing.T) {
	w := NewSessionWindow(5000)
	if _, ok := w.AddCount("u", 0); ok {
		t.Fatalf("session @0: want ok=false")
	}
	if _, ok := w.AddCount("u", 2000); ok {
		t.Fatalf("session @2000: want ok=false")
	}
	s, ok := w.AddCount("u", 10000)
	if !ok {
		t.Fatalf("session @10000: want ok=true")
	}
	if s.Start != 0 || s.End != 2000 || s.Count != 2 {
		t.Fatalf("session emitted: want {Start:0, End:2000, Count:2}, got %+v", s)
	}
	cs, ok := w.Close("u")
	if !ok {
		t.Fatalf("session close: want ok=true")
	}
	if cs.Count != 1 {
		t.Fatalf("session close count: want 1, got %d", cs.Count)
	}
	if _, ok := w.Close("u"); ok {
		t.Fatalf("session close again: want ok=false")
	}
}
