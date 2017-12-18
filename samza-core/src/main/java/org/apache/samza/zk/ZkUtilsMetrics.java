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

/**
 * Contains all the metrics published by {@link ZkUtils}.
 */
public class ZkUtilsMetrics extends MetricsBase {
  /**
   * Number of data reads from zookeeper.
   */
  public final Counter reads;

  /**
   * Number of data writes into zookeeper.
   */
  public final Counter writes;

  /**
   * Number of subscriptions created with zookeeper.
   */
  public final Counter subscriptions;

  /**
   * Number of zookeeper connection errors in ZkClient.
   */
  public final Counter zkConnectionError;

  public ZkUtilsMetrics(MetricsRegistry metricsRegistry) {
    super(metricsRegistry);
    this.reads = newCounter("reads");
    this.writes = newCounter("writes");
    this.subscriptions = newCounter("subscriptions");
    this.zkConnectionError = newCounter("zk-connection-errors");
  }
}
