package org.jagentic.core.privacy;

import java.util.Optional;

import javax.crypto.SecretKey;

/**
 * One data key per conversation, findable by who owns the conversation. The log and the journal hold
 * only what these keys sealed, so destroying a key honours a deletion request on a log nobody can
 * rewrite. This is the SPI a KMS sits behind; {@link FileKeyVault} is the reference.
 */
public interface KeyVault {

  /** The conversation's key, made on first use. {@code owner} is null until the turn that names one. */
  SecretKey keyFor(String conversationId, String owner);

  /** The conversation's key, if it still has one. */
  Optional<SecretKey> existing(String conversationId);

  /** Destroys the key of every conversation {@code owner} owns. Returns how many. */
  int erase(String owner);
}
