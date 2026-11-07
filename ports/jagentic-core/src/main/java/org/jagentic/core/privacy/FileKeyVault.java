package org.jagentic.core.privacy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Keys as files in a directory, one per conversation, with a marker per owner. Every process that
 * handles a conversation needs the same directory. Deleting a file does not scrub it from a disk, a
 * snapshot or a backup: where that matters, keep the keys in a KMS behind {@link KeyVault}.
 */
public final class FileKeyVault implements KeyVault {

  private static final SecureRandom RANDOM = new SecureRandom();

  private final Path dir;

  public FileKeyVault(Path dir) {
    this.dir = dir;
    try {
      Files.createDirectories(dir.resolve("owners"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String name(String value) {
    try {
      return HexFormat.of().formatHex(
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private Path keyFile(String conversationId) {
    return dir.resolve(name(conversationId) + ".key");
  }

  @Override
  public SecretKey keyFor(String conversationId, String owner) {
    Path file = keyFile(conversationId);
    try {
      if (!Files.exists(file)) {
        byte[] key = new byte[32];
        RANDOM.nextBytes(key);
        Path made = Files.createTempFile(dir, "key", ".tmp",
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        Files.write(made, key);
        try {
          Files.move(made, file);
        } catch (FileAlreadyExistsException lost) {
          Files.delete(made);
        }
      }
      if (owner != null) {
        Path marker = dir.resolve("owners").resolve(name(owner));
        Files.createDirectories(marker);
        Path entry = marker.resolve(name(conversationId));
        if (!Files.exists(entry)) {
          Files.writeString(entry, "");
        }
      }
      return new SecretKeySpec(Files.readAllBytes(file), "AES");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Optional<SecretKey> existing(String conversationId) {
    Path file = keyFile(conversationId);
    try {
      return Files.exists(file) ? Optional.of(new SecretKeySpec(Files.readAllBytes(file), "AES")) : Optional.empty();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public int erase(String owner) {
    Path marker = dir.resolve("owners").resolve(name(owner));
    if (!Files.isDirectory(marker)) {
      return 0;
    }
    try (Stream<Path> owned = Files.list(marker)) {
      List<Path> entries = owned.toList();
      for (Path entry : entries) {
        Files.deleteIfExists(dir.resolve(entry.getFileName() + ".key"));
        Files.delete(entry);
      }
      Files.delete(marker);
      return entries.size();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
