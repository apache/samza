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

package org.apache.samza.config;

public class ZkConfig {
  // Connection string for ZK, format: :<hostname>:<port>,..."
  public static final String ZK_CONNECT = "job.coordinator.zk.connect";
  public static final String ZK_SESSION_TIMEOUT_MS = "job.coordinator.zk.session.timeout.ms";
  public static final String ZK_CONNECTION_TIMEOUT_MS = "job.coordinator.zk.connection.timeout.ms";
  public static final String ZK_CONSENSUS_TIMEOUT_MS = "job.coordinator.zk.consensus.timeout.ms";

  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 60000;
  public static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;
  public static final int DEFAULT_CONSENSUS_TIMEOUT_MS = 40000;

  private final Config config;

  public ZkConfig(final Config config) {
    if (null == config) {
      throw new IllegalArgumentException("config cannot be null");
    }
    this.config = config;
  }

  public String getZkConnect() {
    if (!config.containsKey(ZK_CONNECT)) {
      throw new ConfigException("Missing " + ZK_CONNECT + " config!");
    }
    return config.get(ZK_CONNECT);
  }

  public int getZkSessionTimeoutMs() {
    return config.getInt(ZK_SESSION_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS);
  }

  public int getZkConnectionTimeoutMs() {
    return config.getInt(ZK_CONNECTION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  public int getZkBarrierTimeoutMs() {
    return config.getInt(ZK_CONSENSUS_TIMEOUT_MS, DEFAULT_CONSENSUS_TIMEOUT_MS);
  }
}
