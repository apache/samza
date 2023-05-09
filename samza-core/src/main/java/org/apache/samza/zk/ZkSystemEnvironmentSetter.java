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

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.samza.config.Config;


/**
 * Reads the configs to set zookeeper environment variables for TLS.
 * 
 * See https://cwiki.apache.org/confluence/display/zookeeper/zookeeper+ssl+user+guide
 */
public class ZkSystemEnvironmentSetter {

  private static final String SAMZA_PREFIX = "samza.system.";
  private static final String ZOOKEEPER_CLIENT_SECURE = "zookeeper.client.secure";
  private static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";
  private static final String ZOOKEEPER_SSL_KEY_STORE_LOCATION = "zookeeper.ssl.keyStore.location";
  private static final String ZOOKEEPER_SSL_KEY_STORE_PASSWORD = "zookeeper.ssl.keyStore.password";
  private static final String ZOOKEEPER_SSL_KEY_STORE_TYPE = "zookeeper.ssl.keyStore.type";
  private static final String ZOOKEEPER_SSL_TRUST_STORE_LOCATION = "zookeeper.ssl.trustStore.location";
  private static final String ZOOKEEPER_SSL_TRUST_STORE_PASSWORD = "zookeeper.ssl.trustStore.password";
  private static final String ZOOKEEPER_SSL_TRUST_STORE_TYPE = "zookeeper.ssl.trustStore.type";


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
