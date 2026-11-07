package org.jagentic.core.privacy;

import java.util.List;
import java.util.Optional;

import javax.crypto.SecretKey;

import org.jagentic.core.EffectJournal;

/**
 * A journal whose store holds only sealed results and errors. Its intents are SHA-256 digests of the
 * arguments, so low-entropy arguments can be guessed from one, and that survives an erasure.
 */
public final class SealedJournal implements EffectJournal {

  private final EffectJournal inner;
  private final KeyVault vault;
  private final Sealer sealer;

  public SealedJournal(EffectJournal inner, KeyVault vault) {
    this.inner = inner;
    this.vault = vault;
    this.sealer = new Sealer(vault);
  }

  private Entry seal(Entry e) {
    String cid = e.key().conversationId();
    SecretKey key = vault.keyFor(cid, null);
    List<Attempt> attempts = e.attempts().stream()
        .map(a -> new Attempt(a.attempt(), text(sealer.seal(key, cid, a.error())))).toList();
    return new Entry(e.key(), e.intent(), e.effect(), e.state(), attempts,
        e.result() == null ? null : sealer.seal(key, cid, e.result()),
        e.error() == null ? null : text(sealer.seal(key, cid, e.error())), e.released());
  }

  private Entry open(Entry e) {
    String cid = e.key().conversationId();
    Optional<SecretKey> key = vault.existing(cid);
    List<Attempt> attempts = e.attempts().stream()
        .map(a -> new Attempt(a.attempt(), openText(key, cid, a.error()))).toList();
    Object result = Sealer.isSealed(e.result()) ? sealer.open(key, cid, "result", e.result()) : e.result();
    return new Entry(e.key(), e.intent(), e.effect(), e.state(), attempts, result, openText(key, cid, e.error()),
        e.released());
  }

  private static final String PREFIX = "sealed:";

  private static String text(java.util.Map<String, String> sealed) {
    return PREFIX + sealed.get("sealed");
  }

  private String openText(Optional<SecretKey> key, String cid, String stored) {
    if (stored == null || !stored.startsWith(PREFIX)) {
      return stored;
    }
    return String.valueOf(sealer.open(key, cid, "error", java.util.Map.of("sealed", stored.substring(PREFIX.length()))));
  }

  @Override
  public Optional<Entry> get(Key key) {
    return inner.get(key).map(this::open);
  }

  @Override
  public void put(Entry entry) {
    inner.put(seal(entry));
  }

  @Override
  public Optional<Entry> putIfAbsent(Entry entry) {
    return inner.putIfAbsent(seal(entry)).map(this::open);
  }

  @Override
  public boolean durable() {
    return inner.durable();
  }

  @Override
  public void close() {
    inner.close();
  }
}
