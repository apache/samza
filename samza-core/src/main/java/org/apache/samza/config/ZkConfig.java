package org.apache.samza.config;

public class ZkConfig extends MapConfig {
  // Connection string for ZK, format: :<hostname>:<port>,..."
  public static final String ZK_CONNECT = "coordinator.zk.connect";
  public static final String ZK_SESSION_TIMEOUT_MS = "coordinator.zk.session-timeout-ms";
  public static final String ZK_CONNECTION_TIMEOUT_MS = "coordinator.zk.session-timeout-ms";

  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 60000;
  public static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;

  public ZkConfig (Config config) {
    super(config);
  }

  public String getZkConnect() {
    if (!containsKey(ZK_CONNECT)) {
      throw new ConfigException("Missing " + ZK_CONNECT + " config!");
    }
    return get(ZK_CONNECT);
  }

  public int getZkSessionTimeoutMs() {
    return getInt(ZK_SESSION_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS);
  }

  public int getZkConnectionTimeoutMs() {
    return getInt(ZK_CONNECTION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS);
  }
}
