package org.jagentic.pekko.persistence.redis;

import com.typesafe.config.Config;

import org.apache.pekko.persistence.CapabilityFlag;
import org.apache.pekko.persistence.japi.journal.JavaJournalSpec;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.scalatestplus.junit.JUnitRunner;

import org.jagentic.pekko.testing.IntegrationCategory;
import org.jagentic.pekko.testing.RedisContainer;

/**
 * The Pekko persistence journal TCK ({@code pekko-persistence-tck}) run against {@link RedisJournal}
 * on a real Redis. Covers write/replay ranges, highestSequenceNr after deletes, deleteMessagesTo,
 * atomic persistAll batches, serialization and rejection of non-serializable events.
 */
@RunWith(JUnitRunner.class)
@Category(IntegrationCategory.class)
public class RedisJournalTckSpec extends JavaJournalSpec {
  private static final RedisContainer REDIS = RedisContainer.durable();

  private static Config redisConfig() {
    REDIS.start();
    return REDIS.profileConfig("tck-journal-" + System.nanoTime());
  }

  public RedisJournalTckSpec() {
    super(redisConfig());
  }

  @Override
  public CapabilityFlag supportsRejectingNonSerializableObjects() {
    return CapabilityFlag.on();
  }

  @Override
  public CapabilityFlag supportsSerialization() {
    return CapabilityFlag.on();
  }

  @Override
  public void afterAll() {
    super.afterAll();
    REDIS.stop();
  }
}
