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

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsBase;
import org.apache.samza.metrics.MetricsRegistry;

public class ZkSessionMetrics extends MetricsBase {

  /**
   * Denotes the number of times the zookeeper client session had expired.
   */
  public final Counter zkSessionExpirations;

  /**
   *  Denotes the number of times the zookeeper client session disconnects had occurred.
   */
  public final Counter zkSessionDisconnects;

  /**
   * Denotes the number of zookeeper client session errors had occurred.
   */
  public final Counter zkSessionErrors;

  /**
   * Denotes the number of zookeeper client sessions had been established.
   */
  public final Counter zkNewSessions;

  /**
   * Denotes the number of zookeeper sync connected event.
   */
  public final Counter zkSyncConnected;

  public ZkSessionMetrics(MetricsRegistry registry) {
    super(registry);
    this.zkSessionExpirations = newCounter("zk-session-expirations");
    this.zkSessionDisconnects = newCounter("zk-session-disconnects");
    this.zkSessionErrors = newCounter("zk-session-errors");
    this.zkNewSessions = newCounter("zk-new-sessions");
    this.zkSyncConnected = newCounter("zk-sync-connected");
  }
}
