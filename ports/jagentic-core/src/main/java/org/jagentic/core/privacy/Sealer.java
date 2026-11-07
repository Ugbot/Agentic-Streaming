package org.jagentic.core.privacy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * AES-GCM over the fields of an event that carry what someone said or did. What the fold needs to
 * tell one event from another stays readable: types, sequences, paths, tool ids and the owner's
 * identifier. A value that can no longer be opened reads as {@link #ERASED}, or as an empty {@code
 * args}, {@code signal} or {@code messages}, so the log still folds.
 */
final class Sealer {

  static final String ERASED = "[erased]";

  private static final Set<String> FIELDS =
      Set.of("text", "reply", "args", "result", "messages", "signal", "error", "reason");
  private static final String TAG = "sealed";
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final SecureRandom RANDOM = new SecureRandom();

  private final KeyVault vault;

  Sealer(KeyVault vault) {
    this.vault = vault;
  }

  Map<String, Object> seal(String conversationId, String owner, Map<String, Object> payload) {
    Map<String, Object> out = new LinkedHashMap<>(payload);
    SecretKey key = vault.keyFor(conversationId, owner);
    for (String field : FIELDS) {
      if (out.get(field) != null) {
        out.put(field, seal(key, conversationId, out.get(field)));
      }
    }
    return out;
  }

  Map<String, Object> open(String conversationId, Map<String, Object> payload) {
    Map<String, Object> out = new LinkedHashMap<>(payload);
    Optional<SecretKey> key = vault.existing(conversationId);
    for (String field : FIELDS) {
      if (isSealed(out.get(field))) {
        out.put(field, open(key, conversationId, field, out.get(field)));
      }
    }
    return out;
  }

  Map<String, String> seal(SecretKey key, String conversationId, Object value) {
    try {
      byte[] iv = new byte[12];
      RANDOM.nextBytes(iv);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, iv));
      cipher.updateAAD(conversationId.getBytes(StandardCharsets.UTF_8));
      byte[] sealed = cipher.doFinal(JSON.writeValueAsBytes(value));
      return Map.of(TAG, Base64.getEncoder().encodeToString(
          ByteBuffer.allocate(iv.length + sealed.length).put(iv).put(sealed).array()));
    } catch (GeneralSecurityException | JsonProcessingException e) {
      throw new IllegalStateException("could not seal a value", e);
    }
  }

  static boolean isSealed(Object value) {
    return value instanceof Map<?, ?> m && m.size() == 1 && m.get(TAG) instanceof String;
  }

  /** The value, or what an erased one of that field reads as. */
  Object open(Optional<SecretKey> key, String conversationId, String field, Object sealed) {
    if (key.isEmpty()) {
      return erased(field);
    }
    try {
      byte[] all = Base64.getDecoder().decode((String) ((Map<?, ?>) sealed).get(TAG));
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE, key.get(), new GCMParameterSpec(128, all, 0, 12));
      cipher.updateAAD(conversationId.getBytes(StandardCharsets.UTF_8));
      return JSON.readValue(cipher.doFinal(all, 12, all.length - 12), Object.class);
    } catch (GeneralSecurityException | java.io.IOException | IllegalArgumentException e) {
      return erased(field); // sealed under a key that was destroyed; a new key cannot open it
    }
  }

  private static Object erased(String field) {
    return switch (field) {
      case "args", "signal" -> Map.of();
      case "messages" -> List.of();
      default -> ERASED;
    };
  }
}
