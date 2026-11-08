package org.jagentic.core.privacy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.jagentic.core.TurnResult;

/**
 * A turn's result as it goes on a results or failures topic, under the conversation's key. A result
 * carries the turn's text, reply, events and state, and a topic cannot be rewritten, so with a key
 * vault the whole result is sealed and only the ids and the status stay readable. Destroying the key
 * ({@link Erase}) leaves ciphertext nobody can open, on the topic and in anything rebuilt from it,
 * such as the gateway's view. A result of a conversation that has no key, such as a turn refused
 * before anything was logged, goes out as a reference: ids, status and error class, nothing said.
 */
public final class SealedResults {

  /** The field that holds the sealed result. */
  public static final String FIELD = "sealed_result";

  private static final SecureRandom RANDOM = new SecureRandom();

  private SealedResults() {}

  /** The record for {@code result}, whose encoded form is {@code encoded}. */
  public static Map<String, Object> seal(KeyVault vault, TurnResult result, byte[] encoded) {
    Map<String, Object> out = reference(result);
    Optional<SecretKey> key = vault.existing(result.conversationId);
    if (key.isPresent()) {
      out.put(FIELD, encrypt(key.get(), result.conversationId, encoded));
    }
    return out;
  }

  /** Ids, status and error class: what is left when there is nothing to seal under. */
  public static Map<String, Object> reference(TurnResult result) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("conversation_id", result.conversationId);
    out.put("turn_id", result.turnId);
    out.put("status", result.status.wire());
    if (result.error != null) {
      out.put("error", Map.of("class", result.error.errorClass().wire()));
    }
    out.put("sealed", true);
    return out;
  }

  /** True for a record this class wrote. */
  public static boolean isSealed(Map<?, ?> record) {
    return Boolean.TRUE.equals(record.get("sealed"));
  }

  /** The encoded result inside {@code record}, or empty when it is a reference or its key is gone. */
  public static Optional<byte[]> open(KeyVault vault, Map<?, ?> record) {
    if (vault == null || !(record.get(FIELD) instanceof String sealed)
        || !(record.get("conversation_id") instanceof String cid)) {
      return Optional.empty();
    }
    Optional<SecretKey> key = vault.existing(cid);
    if (key.isEmpty()) {
      return Optional.empty();
    }
    try {
      byte[] all = Base64.getDecoder().decode(sealed);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE, key.get(), new GCMParameterSpec(128, all, 0, 12));
      cipher.updateAAD(aad(cid));
      return Optional.of(cipher.doFinal(all, 12, all.length - 12));
    } catch (GeneralSecurityException | IllegalArgumentException e) {
      return Optional.empty(); // sealed under a key that was destroyed; a new key cannot open it
    }
  }

  private static String encrypt(SecretKey key, String cid, byte[] plain) {
    try {
      byte[] iv = new byte[12];
      RANDOM.nextBytes(iv);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, iv));
      cipher.updateAAD(aad(cid));
      byte[] sealed = cipher.doFinal(plain);
      return Base64.getEncoder().encodeToString(ByteBuffer.allocate(12 + sealed.length).put(iv).put(sealed).array());
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("could not seal a result", e);
    }
  }

  private static byte[] aad(String cid) {
    return ("result:" + cid).getBytes(StandardCharsets.UTF_8);
  }
}
