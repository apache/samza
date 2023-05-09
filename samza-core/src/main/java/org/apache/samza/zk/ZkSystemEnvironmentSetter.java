package org.apache.samza.zk;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.config.Config;


/**
  Reads the configs to set zookeeper environment variables for TLS.
 */
public class ZkSystemEnvironmentSetter {

  private static final String SAMZA_PREFIX = "samza.system.";
  private static final String ZOOKEEPER_CLIENT_SECURE = "zookeeper.client.secure";
  public static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";
  public static final String ZOOKEEPER_SSL_KEY_STORE_LOCATION = "zookeeper.ssl.keyStore.location";
  public static final String ZOOKEEPER_SSL_KEY_STORE_PASSWORD = "zookeeper.ssl.keyStore.password";
  public static final String ZOOKEEPER_SSL_KEY_STORE_TYPE = "zookeeper.ssl.keyStore.type";
  public static final String ZOOKEEPER_SSL_TRUST_STORE_LOCATION = "zookeeper.ssl.trustStore.location";
  public static final String ZOOKEEPER_SSL_TRUST_STORE_PASSWORD = "zookeeper.ssl.trustStore.password";
  public static final String ZOOKEEPER_SSL_TRUST_STORE_TYPE = "zookeeper.ssl.trustStore.type";


  private static final List<String> ZOOKEEPER_SSL_KEYS = ImmutableList.of(
    ZOOKEEPER_CLIENT_SECURE,
    ZOOKEEPER_CLIENT_CNXN_SOCKET,
    ZOOKEEPER_SSL_KEY_STORE_LOCATION,
    ZOOKEEPER_SSL_KEY_STORE_PASSWORD,
    ZOOKEEPER_SSL_KEY_STORE_TYPE,
    ZOOKEEPER_SSL_TRUST_STORE_LOCATION,
    ZOOKEEPER_SSL_TRUST_STORE_PASSWORD,
    ZOOKEEPER_SSL_TRUST_STORE_TYPE);

  private ZkSystemEnvironmentSetter(){}

  public static void setZkEnvironment(Config config) {
    Map<String, String> zookeeperSettings = new HashMap<>();
    for(String key: ZOOKEEPER_SSL_KEYS){
      Optional<String> override =
          Optional.ofNullable(config.get(SAMZA_PREFIX + key));
      if(override.isPresent()) {
        zookeeperSettings.put(key, override.get());
      }
    }

    zookeeperSettings.entrySet().stream()
        .forEach(entry -> System.setProperty(entry.getKey(), entry.getValue()));
  }
}
