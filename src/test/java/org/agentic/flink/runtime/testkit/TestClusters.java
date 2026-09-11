package org.agentic.flink.runtime.testkit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

/** Starts an in-JVM {@link MiniCluster} without a REST endpoint for the runtime tests. */
public final class TestClusters {
  private TestClusters() {}

  public static MiniCluster start(int slots) throws Exception {
    Configuration conf = new Configuration();
    conf.set(RestOptions.BIND_PORT, "0");
    MiniCluster cluster = new MiniCluster(new MiniClusterConfiguration.Builder()
        .setConfiguration(conf)
        .setNumTaskManagers(1)
        .setNumSlotsPerTaskManager(slots)
        .build());
    cluster.start();
    return cluster;
  }
}
