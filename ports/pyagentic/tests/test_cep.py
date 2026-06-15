"""Phase 4 (headline): portable CEP. Mirrors the Java ``CepMatcherTest`` goldens — the
incident pattern "3 anomalies within 5 minutes on one host", plus strict/relaxed contiguity,
within-expiry, iterative conditions, and the observer bridge."""

from __future__ import annotations

from pyagentic import Event
from pyagentic.cep import CepMatcher, CepObserver, Pattern, any_, simple

FIVE_MIN = 5 * 60 * 1000


def anomaly(host: str) -> Event:
    """An anomaly event for a host (conversation_id carries the host for the key)."""
    return Event(host, "anomaly", user_id="monitor", metadata={"metric": "cpu"})


def incident_pattern() -> Pattern:
    return (
        Pattern.begin("first", any_())
        .followed_by("second", any_())
        .followed_by("third", any_())
        .within(FIVE_MIN)
    )


def test_three_anomalies_in_five_minutes_fire_one_incident() -> None:
    m = CepMatcher(incident_pattern())
    assert m.match("h1", 0, anomaly("h1")) == []
    assert m.match("h1", 60_000, anomaly("h1")) == []
    done = m.match("h1", 120_000, anomaly("h1"))  # 3rd within 5 min
    assert len(done) == 1
    incident = done[0]
    assert len(incident.events) == 3
    assert list(incident.named.keys()) == ["first", "second", "third"]


def test_keys_are_independent() -> None:
    m = CepMatcher(incident_pattern())
    m.match("h1", 0, anomaly("h1"))
    m.match("h2", 0, anomaly("h2"))
    m.match("h1", 1000, anomaly("h1"))
    assert m.match("h2", 1000, anomaly("h2")) == [], "h2 only has 2"
    assert len(m.match("h1", 2000, anomaly("h1"))) == 1, "h1 reaches 3"


def test_within_expiry_prevents_late_completion() -> None:
    m = CepMatcher(incident_pattern())
    m.match("h1", 0, anomaly("h1"))
    m.match("h1", 60_000, anomaly("h1"))
    # 3rd arrives after the 5-minute window from the first -> partials expired, no completion
    assert m.match("h1", FIVE_MIN + 1, anomaly("h1")) == []
    # flush_expired surfaces the timed-out partial(s)
    timed_out = m.flush_expired("h1", FIVE_MIN + 100_000)
    assert len(timed_out) >= 1, "the stalled partial is reported as timed out"


def test_strict_contiguity_drops_on_interleaved_non_match() -> None:
    # begin A, NEXT B(text=="b"): an interleaved non-"b" event breaks the strict partial.
    p = Pattern.begin("a", simple(lambda e: e.text == "a")).next(
        "b", simple(lambda e: e.text == "b")
    )
    m = CepMatcher(p)
    m.match("k", 0, Event("k", "a", user_id="u"))
    assert m.match("k", 1, Event("k", "x", user_id="u")) == [], "strict: 'x' breaks it"
    assert m.match("k", 2, Event("k", "b", user_id="u")) == [], "partial already dropped"


def test_relaxed_contiguity_skips_non_matches() -> None:
    p = Pattern.begin("a", simple(lambda e: e.text == "a")).followed_by(
        "b", simple(lambda e: e.text == "b")
    )
    m = CepMatcher(p)
    m.match("k", 0, Event("k", "a", user_id="u"))
    assert m.match("k", 1, Event("k", "x", user_id="u")) == [], "relaxed: 'x' is skipped"
    assert len(m.match("k", 2, Event("k", "b", user_id="u"))) == 1, "'b' completes after the skip"


def test_iterative_condition_sees_matched_so_far() -> None:
    # second event must have the same host (conversation_id) as the first.
    p = Pattern.begin("a", any_()).followed_by(
        "b", lambda e, so_far: e.conversation_id == so_far[0].conversation_id
    )
    m = CepMatcher(p)
    m.match("h1", 0, anomaly("h1"))
    assert len(m.match("h1", 1, anomaly("h1"))) == 1


def test_observer_bridge_fires_handler_on_match() -> None:
    incidents = [0]

    def on_match(_match) -> None:
        incidents[0] += 1

    obs = CepObserver(
        incident_pattern(),
        key_fn=lambda e: e.conversation_id,
        ts_fn=lambda e: int(e.metadata.get("ts", "0")),
        on_match=on_match,
    )
    for t in ("0", "1000", "2000"):
        obs(Event("h1", "anomaly", user_id="monitor", metadata={"ts": t}))
    assert incidents[0] == 1
