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

package org.apache.samza.storage.blobstore.metrics;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;


public class BlobStoreRestoreManagerMetrics {
  private static final String GROUP = BlobStoreRestoreManagerMetrics.class.getName();
  private final MetricsRegistry metricsRegistry;

  // TODO LOW shesharma per-task throughput
  public final Gauge<Long> initNs;
  public final Gauge<Long> getSnapshotIndexNs;

  public final Gauge<Long> restoreNs;
  // gauges of AtomicLong so that the value can be incremented/decremented atomically in a thread-safe way.
  // don't set the gauge value directly. use gauge.getValue().incrementAndGet() etc instead.
  public final Gauge<AtomicLong> filesToRestore;
  public final Gauge<AtomicLong> bytesToRestore;
  public final Gauge<AtomicLong> filesRestored;
  public final Gauge<AtomicLong> bytesRestored;
  public final Gauge<AtomicLong> filesRemaining;
  public final Gauge<AtomicLong> bytesRemaining;

  public final Counter restoreRate;

  // per store breakdowns
  public final Map<String, Gauge<Long>> storePreRestoreNs;
  public final Map<String, Gauge<Long>> storeRestoreNs;
  public final Map<String, Gauge<Long>> storePostRestoreNs;

  // TODO LOW shesharma move to SamzaHistogram
  public final Timer avgFileRestoreNs; // avg time for each file restored

  public BlobStoreRestoreManagerMetrics(MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;

    this.initNs = metricsRegistry.newGauge(GROUP, "init-ns", 0L);
    this.getSnapshotIndexNs = metricsRegistry.newGauge(GROUP, "get-snapshot-index-ns", 0L);

    this.restoreNs = metricsRegistry.newGauge(GROUP, "restore-ns", 0L);
    this.filesToRestore = metricsRegistry.newGauge(GROUP, "files-to-restore", new AtomicLong());
    this.bytesToRestore = metricsRegistry.newGauge(GROUP, "bytes-to-restore", new AtomicLong());
    this.filesRestored = metricsRegistry.newGauge(GROUP, "files-restored", new AtomicLong());
    this.bytesRestored = metricsRegistry.newGauge(GROUP, "bytes-restored", new AtomicLong());
    this.filesRemaining = metricsRegistry.newGauge(GROUP, "files-remaining", new AtomicLong());
    this.bytesRemaining = metricsRegistry.newGauge(GROUP, "bytes-remaining", new AtomicLong());


    this.restoreRate = metricsRegistry.newCounter(GROUP, "restore-rate");

    this.storePreRestoreNs = new ConcurrentHashMap<>();
    this.storeRestoreNs = new ConcurrentHashMap<>();
    this.storePostRestoreNs = new ConcurrentHashMap<>();

    this.avgFileRestoreNs = metricsRegistry.newTimer(GROUP, "avg-file-restore-ns");
  }

  public void initStoreMetrics(Collection<String> storeNames) {
    for (String storeName: storeNames) {
      storePreRestoreNs.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-pre-restore-ns", storeName), 0L));
      storeRestoreNs.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-restore-ns", storeName), 0L));
      storePostRestoreNs.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-post-restore-ns", storeName), 0L));
    }
  }
}
