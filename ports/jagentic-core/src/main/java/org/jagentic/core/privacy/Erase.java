package org.jagentic.core.privacy;

import java.nio.file.Path;

/**
 * Honours a deletion request: destroys the keys of every conversation one owner owns. The owner is
 * {@code tenant/subject}, or the user id when a turn had no tenant.
 *
 * <pre>AGENTIC_KEY_VAULT_DIR=/keys java -cp ... org.jagentic.core.privacy.Erase acme/alice</pre>
 */
public final class Erase {

  private Erase() {}

  public static void main(String[] args) {
    String dir = System.getenv("AGENTIC_KEY_VAULT_DIR");
    if (dir == null || args.length != 1) {
      System.err.println("set AGENTIC_KEY_VAULT_DIR and name one owner");
      System.exit(2);
    }
    System.out.println("erased " + new FileKeyVault(Path.of(dir)).erase(args[0]) + " conversation(s)");
  }
}
