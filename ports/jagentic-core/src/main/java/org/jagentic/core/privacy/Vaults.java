package org.jagentic.core.privacy;

import java.nio.file.Path;

/** The operator's key vault: {@code AGENTIC_KEY_VAULT_DIR}, or the system property {@code agentic.key_vault_dir}. */
public final class Vaults {

  private Vaults() {}

  /** The vault this process is configured with, or null: then nothing is sealed. */
  public static KeyVault fromEnvironment() {
    String dir = System.getProperty("agentic.key_vault_dir", System.getenv("AGENTIC_KEY_VAULT_DIR"));
    return dir == null || dir.isBlank() ? null : new FileKeyVault(Path.of(dir));
  }
}
