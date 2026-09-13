package org.jagentic.pekko.persistence.redis;

import com.typesafe.config.Config;

import org.apache.pekko.persistence.CapabilityFlag;
import org.apache.pekko.persistence.japi.snapshot.JavaSnapshotStoreSpec;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.scalatestplus.junit.JUnitRunner;

import org.jagentic.pekko.testing.IntegrationCategory;
import org.jagentic.pekko.testing.RedisContainer;

/** The Pekko persistence snapshot-store TCK run against {@link RedisSnapshotStore} on a real Redis. */
@RunWith(JUnitRunner.class)
@Category(IntegrationCategory.class)
public class RedisSnapshotStoreTckSpec extends JavaSnapshotStoreSpec {
  private static final RedisContainer REDIS = RedisContainer.durable();

  private static Config redisConfig() {
    REDIS.start();
    return REDIS.profileConfig("tck-snapshot-" + System.nanoTime());
  }

  public RedisSnapshotStoreTckSpec() {
    super(redisConfig());
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
