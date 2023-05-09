package org.apache.samza.zk;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestZkSystemEnvironmentSetter {
  @Test
  public void testDefaultConfig(){
    Map<String, String> configMap= new HashMap<>();
    configMap.put("samza.zookeeper.client.secure", "true");
    configMap.put("samza.zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
    configMap.put("samza.zookeeper.ssl.keyStore.location", "keyStoreLocation");
    configMap.put("samza.zookeeper.ssl.keyStore.password", "keyStorePassword");
    configMap.put("samza.zookeeper.ssl.keyStore.type", "PKCS12");
    configMap.put("samza.zookeeper.ssl.trustStore.location", "trustStoreLocation");
    configMap.put("samza.zookeeper.ssl.trustStore.password", "trustStorePassword");
    configMap.put("samza.zookeeper.ssl.trustStore.type", "JKS");
    Config config = new MapConfig(configMap);

    ZkSystemEnvironmentSetter.setZkEnvironment(config);
    assertEquals(System.getProperty("zookeeper.client.secure"), "true");
    assertEquals(System.getProperty("zookeeper.clientCnxnSocket"), "org.apache.zookeeper.ClientCnxnSocketNetty");
    assertEquals(System.getProperty("zookeeper.ssl.keyStore.location"), "keyStoreLocation");
    assertEquals(System.getProperty("zookeeper.ssl.keyStore.password"), "keyStorePassword");
    assertEquals(System.getProperty("zookeeper.ssl.keyStore.type"), "PKCS12");
    assertEquals(System.getProperty("zookeeper.ssl.trustStore.location"), "trustStoreLocation");
    assertEquals(System.getProperty("zookeeper.ssl.trustStore.password"), "trustStorePassword");
    assertEquals(System.getProperty("zookeeper.ssl.trustStore.type"), "JKS");
  }
}
