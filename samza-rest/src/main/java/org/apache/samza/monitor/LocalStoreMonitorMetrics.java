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
package org.apache.samza.monitor;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsBase;
import org.apache.samza.metrics.MetricsRegistry;

/**
 * Contains all the metrics published by {@link LocalStoreMonitor}.
 */
public class LocalStoreMonitorMetrics extends MetricsBase {

  /** Total number of task partition stores deleted by the LocalStoreMonitor. */
  public final Counter noOfDeletedTaskPartitionStores;

  /** Total disk space cleared by the LocalStoreMonitor. */
  public final Counter diskSpaceFreedInBytes;

  /** Total number of times task store deletions have been attempted and failed. */
  public final Counter failedStoreDeletionAttempts;

  public LocalStoreMonitorMetrics(String prefix, MetricsRegistry registry) {
    super(prefix, registry);
    diskSpaceFreedInBytes = newCounter("diskSpaceFreedInBytes");
    noOfDeletedTaskPartitionStores = newCounter("noOfDeletedTaskPartitionStores");
    failedStoreDeletionAttempts = newCounter("failedStoreDeletionAttempts");
  }
}
