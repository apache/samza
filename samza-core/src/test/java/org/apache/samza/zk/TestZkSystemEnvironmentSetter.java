/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.zk;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestZkSystemEnvironmentSetter {
  @Test
  public void testDefaultConfig() {
    Map<String, String> configMap= new HashMap<>();
    configMap.put("samza.system.zookeeper.client.secure", "true");
    configMap.put("samza.system.zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
    configMap.put("samza.system.zookeeper.ssl.keyStore.location", "keyStoreLocation");
    configMap.put("samza.system.zookeeper.ssl.keyStore.password", "keyStorePassword");
    configMap.put("samza.system.zookeeper.ssl.keyStore.type", "PKCS12");
    configMap.put("samza.system.zookeeper.ssl.trustStore.location", "trustStoreLocation");
    configMap.put("samza.system.zookeeper.ssl.trustStore.password", "trustStorePassword");
    configMap.put("samza.system.zookeeper.ssl.trustStore.type", "JKS");
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
