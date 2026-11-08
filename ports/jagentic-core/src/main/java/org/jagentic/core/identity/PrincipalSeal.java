package org.jagentic.core.identity;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.jagentic.core.Event;
import org.jagentic.core.Principal;

/**
 * A principal the gateway signed, bound to one turn, so that whoever can write a runtime's input
 * topic cannot claim one (spec section 11, signed principals).
 *
 * <p>The gateway holds an Ed25519 private key and signs, for every turn and signal it delivers, the
 * principal (subject, tenant, roles, issuer), the conversation key and turn id it is for, a SHA-256
 * digest of the turn's text, metadata and signal, an expiry and a random id. The runtime holds only
 * the public keys, so it can check a principal and cannot make one. The proof travels beside the
 * event as {@code principal_proof}: {@code v1.<claims>.<signature>}, both base64url.
 *
 * <p>Keys are PEM: the private key as PKCS#8 ({@code openssl genpkey -algorithm ed25519}), the
 * public keys as SubjectPublicKeyInfo ({@code openssl pkey -pubout}), one or more in a file, so that
 * a new key can be added before the gateway signs with it and the old one removed after.
 */
public final class PrincipalSeal {

  /** How long a signed principal is good for when the gateway is not told otherwise. */
  public static final Duration DEFAULT_TTL = Duration.ofMinutes(5);

  /** The clock difference allowed between a gateway and a runtime. */
  public static final Duration SKEW = Duration.ofSeconds(60);

  private static final String VERSION = "v1";
  private static final ObjectMapper JSON = new ObjectMapper()
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};
  private static final Pattern PEM =
      Pattern.compile("-----BEGIN ([A-Z ]+)-----([A-Za-z0-9+/=\\s]+)-----END \\1-----");
  private static final SecureRandom RANDOM = new SecureRandom();

  private PrincipalSeal() {}

  /** What a proof says. */
  public record Claims(Principal principal, String conversationId, String turnId, String digest,
                       long expiresAt, String id) {}

  /** The digest of what a turn says: its text, metadata and signal, in a canonical JSON form. */
  public static String digest(Event event) {
    Map<String, Object> m = new TreeMap<>();
    m.put("text", event.text() == null ? "" : event.text());
    m.put("metadata", new TreeMap<>(event.metadata() == null ? Map.of() : event.metadata()));
    m.put("signal", event.signal());
    try {
      byte[] canonical = JSON.writeValueAsBytes(m);
      return b64(MessageDigest.getInstance("SHA-256").digest(canonical));
    } catch (JsonProcessingException | GeneralSecurityException e) {
      throw new IllegalStateException("cannot digest a turn", e);
    }
  }

  /** The gateway's half: signs events with the private key. */
  public static final class Signer {
    private final PrivateKey key;
    private final Duration ttl;

    public Signer(PrivateKey key, Duration ttl) {
      this.key = Objects.requireNonNull(key, "key");
      this.ttl = ttl == null ? DEFAULT_TTL : ttl;
    }

    /** From a PKCS#8 PEM file. */
    public static Signer fromFile(Path pem, Duration ttl) throws IOException {
      return new Signer(privateKey(Files.readString(pem, StandardCharsets.UTF_8)), ttl);
    }

    /** The proof for {@code event}, whose principal must be set. */
    public String sign(Event event, Instant now) {
      Principal p = Objects.requireNonNull(event.principal(), "only an authenticated event is signed");
      Map<String, Object> claims = new LinkedHashMap<>();
      claims.put("sub", p.subject());
      claims.put("ten", p.tenant());
      claims.put("rol", p.roles());
      claims.put("iss", p.issuer());
      claims.put("cid", event.conversationId());
      claims.put("tid", event.turnId());
      claims.put("dig", digest(event));
      claims.put("iat", now.getEpochSecond());
      claims.put("exp", now.plus(ttl).getEpochSecond());
      byte[] id = new byte[16];
      RANDOM.nextBytes(id);
      claims.put("jti", b64(id));
      try {
        String signed = VERSION + "." + b64(JSON.writeValueAsBytes(claims));
        Signature s = Signature.getInstance("Ed25519");
        s.initSign(key);
        s.update(signed.getBytes(StandardCharsets.US_ASCII));
        return signed + "." + b64(s.sign());
      } catch (JsonProcessingException | GeneralSecurityException e) {
        throw new IllegalStateException("cannot sign a principal", e);
      }
    }
  }

  /** The runtime's half: checks proofs against the public keys. Serializable, so a job can carry it. */
  public static final class Verifier implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<byte[]> keys; // X.509 SubjectPublicKeyInfo
    private transient List<PublicKey> decoded;

    public Verifier(List<PublicKey> keys) {
      if (keys == null || keys.isEmpty()) {
        throw new IllegalArgumentException("a verifier needs at least one public key");
      }
      this.keys = keys.stream().map(PublicKey::getEncoded).toList();
    }

    /** From PEM text holding one or more public keys. */
    public static Verifier fromPem(String pem) {
      List<PublicKey> keys = new ArrayList<>();
      Matcher m = PEM.matcher(pem);
      while (m.find()) {
        if (m.group(1).equals("PUBLIC KEY")) {
          keys.add(publicKey(der(m.group(2))));
        }
      }
      if (keys.isEmpty()) {
        throw new IllegalArgumentException("no PUBLIC KEY in the principal keys");
      }
      return new Verifier(keys);
    }

    /** The keys as PEM, which is how they travel inside a job. */
    public String pem() {
      StringBuilder out = new StringBuilder();
      for (byte[] k : keys) {
        out.append("-----BEGIN PUBLIC KEY-----\n").append(Base64.getMimeEncoder().encodeToString(k))
            .append("\n-----END PUBLIC KEY-----\n");
      }
      return out.toString();
    }

    private List<PublicKey> decoded() {
      if (decoded == null) {
        decoded = keys.stream().map(PrincipalSeal::publicKey).toList();
      }
      return decoded;
    }

    /**
     * Why {@code event} may not be believed, or null when its principal is the one the gateway
     * signed for this very turn. {@code allowExpired} is for a turn already on the log, delivered
     * again: it is the same turn, and an expiry is no reason to answer it differently.
     */
    public String problem(Event event, Instant now, boolean allowExpired) {
      Claims c;
      try {
        c = claims(event.principalProof());
      } catch (IllegalArgumentException e) {
        return e.getMessage();
      }
      if (!Objects.equals(c.conversationId(), event.conversationId())
          || !Objects.equals(c.turnId(), event.turnId())) {
        return "the signed principal is for another turn";
      }
      if (!Objects.equals(c.digest(), digest(event))) {
        return "the turn is not the one that was signed";
      }
      Principal on = event.principal();
      Principal signed = c.principal();
      if (on == null || !on.subject().equals(signed.subject()) || !Objects.equals(on.tenant(), signed.tenant())
          || !Objects.equals(on.issuer(), signed.issuer()) || !on.roles().equals(signed.roles())) {
        return "the principal is not the one that was signed";
      }
      if (!allowExpired && now.getEpochSecond() > c.expiresAt() + SKEW.toSeconds()) {
        return "the signed principal has expired";
      }
      return null;
    }

    /** The claims of a proof whose signature verifies under one of the keys; throws otherwise. */
    public Claims claims(String proof) {
      if (proof == null || proof.isBlank()) {
        throw new IllegalArgumentException("the principal is not signed");
      }
      String[] parts = proof.split("\\.", -1);
      if (parts.length != 3 || !parts[0].equals(VERSION)) {
        throw new IllegalArgumentException("the principal's proof is malformed");
      }
      byte[] signature;
      Map<String, Object> m;
      try {
        signature = Base64.getUrlDecoder().decode(parts[2]);
        m = JSON.readValue(Base64.getUrlDecoder().decode(parts[1]), OBJECT);
      } catch (IllegalArgumentException | IOException e) {
        throw new IllegalArgumentException("the principal's proof is malformed");
      }
      byte[] signed = (parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII);
      boolean ok = false;
      for (PublicKey key : decoded()) {
        try {
          Signature s = Signature.getInstance("Ed25519");
          s.initVerify(key);
          s.update(signed);
          if (s.verify(signature)) {
            ok = true;
            break;
          }
        } catch (GeneralSecurityException e) {
          // not this key
        }
      }
      if (!ok) {
        throw new IllegalArgumentException("the principal's signature does not verify");
      }
      try {
        List<String> roles = m.get("rol") instanceof List<?> l ? l.stream().map(String::valueOf).toList() : List.of();
        Principal p = new Principal(text(m.get("sub")), text(m.get("ten")), roles, text(m.get("iss")));
        return new Claims(p, text(m.get("cid")), text(m.get("tid")), text(m.get("dig")),
            ((Number) m.get("exp")).longValue(), text(m.get("jti")));
      } catch (RuntimeException e) {
        throw new IllegalArgumentException("the principal's proof is malformed");
      }
    }
  }

  /** The id of a proof, unchecked: for telling one answer from another once it has been checked. */
  public static String idOf(String proof) {
    try {
      String[] parts = proof.split("\\.", -1);
      return text(JSON.readValue(Base64.getUrlDecoder().decode(parts[1]), OBJECT).get("jti"));
    } catch (RuntimeException | IOException e) {
      return null;
    }
  }

  static PrivateKey privateKey(String pem) {
    Matcher m = PEM.matcher(pem);
    while (m.find()) {
      if (m.group(1).equals("PRIVATE KEY")) {
        try {
          return KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(der(m.group(2))));
        } catch (GeneralSecurityException e) {
          throw new IllegalArgumentException("the principal signing key is not an Ed25519 PKCS#8 key", e);
        }
      }
    }
    throw new IllegalArgumentException("no PRIVATE KEY in the principal signing key");
  }

  static PublicKey publicKey(byte[] der) {
    try {
      return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(der));
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException("a principal key is not an Ed25519 public key", e);
    }
  }

  private static byte[] der(String base64) {
    return Base64.getMimeDecoder().decode(base64.replaceAll("\\s", ""));
  }

  private static String b64(byte[] b) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
  }

  private static String text(Object o) {
    return o == null ? null : String.valueOf(o);
  }
}
