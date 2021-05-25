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


public class BlobStoreBackupManagerMetrics {
  private static final String GROUP = BlobStoreBackupManagerMetrics.class.getName();
  private final MetricsRegistry metricsRegistry;

  // TODO LOW shesharm  per-task throughput
  public final Gauge<Long> initNs;

  public final Timer uploadNs;
  // gauges of AtomicLong so that the value can be incremented/decremented atomically in a thread-safe way.
  // don't set the gauge value directly. use gauge.getValue().incrementAndGet() etc instead.
  public final Gauge<AtomicLong> filesToUpload;
  public final Gauge<AtomicLong> bytesToUpload;
  public final Gauge<AtomicLong> filesUploaded;
  public final Gauge<AtomicLong> bytesUploaded;
  public final Gauge<AtomicLong> filesRemaining;
  public final Gauge<AtomicLong> bytesRemaining;
  public final Gauge<AtomicLong> filesToRetain;
  public final Gauge<AtomicLong> bytesToRetain;
  public final Counter uploadRate;

  // per store breakdowns
  public final Map<String, Timer> storeDirDiffNs;
  public final Map<String, Timer> storeUploadNs;

  public final Map<String, Gauge<Long>> storeFilesToUpload;
  public final Map<String, Gauge<Long>> storeFilesToRetain;
  public final Map<String, Gauge<Long>> storeFilesToRemove;
  public final Map<String, Gauge<Long>> storeSubDirsToUpload;
  public final Map<String, Gauge<Long>> storeSubDirsToRetain;
  public final Map<String, Gauge<Long>> storeSubDirsToRemove;
  public final Map<String, Gauge<Long>> storeBytesToUpload;
  public final Map<String, Gauge<Long>> storeBytesToRetain;
  public final Map<String, Gauge<Long>> storeBytesToRemove;

  public final Timer cleanupNs;

  // TODO shesharm LOW move to SamzaHistogram
  public final Timer avgFileUploadNs; // avg time for each file uploaded
  public final Timer avgFileSizeBytes; // avg size of each file uploaded

  public BlobStoreBackupManagerMetrics(MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;

    this.initNs = metricsRegistry.newGauge(GROUP, "init-ns", 0L);

    this.uploadNs = metricsRegistry.newTimer(GROUP, "upload-ns");

    this.filesToUpload = metricsRegistry.newGauge(GROUP, "files-to-upload", new AtomicLong(0L));
    this.bytesToUpload = metricsRegistry.newGauge(GROUP, "bytes-to-upload", new AtomicLong(0L));
    this.filesUploaded = metricsRegistry.newGauge(GROUP, "files-uploaded", new AtomicLong(0L));
    this.bytesUploaded = metricsRegistry.newGauge(GROUP, "bytes-uploaded", new AtomicLong(0L));
    this.filesRemaining = metricsRegistry.newGauge(GROUP, "files-remaining", new AtomicLong(0L));
    this.bytesRemaining = metricsRegistry.newGauge(GROUP, "bytes-remaining", new AtomicLong(0L));
    this.filesToRetain = metricsRegistry.newGauge(GROUP, "files-to-retain", new AtomicLong(0L));
    this.bytesToRetain = metricsRegistry.newGauge(GROUP, "bytes-to-retain", new AtomicLong(0L));

    this.storeDirDiffNs = new ConcurrentHashMap<>();
    this.storeUploadNs = new ConcurrentHashMap<>();

    this.storeFilesToUpload = new ConcurrentHashMap<>();
    this.storeFilesToRetain = new ConcurrentHashMap<>();
    this.storeFilesToRemove = new ConcurrentHashMap<>();
    this.storeSubDirsToUpload = new ConcurrentHashMap<>();
    this.storeSubDirsToRetain = new ConcurrentHashMap<>();
    this.storeSubDirsToRemove = new ConcurrentHashMap<>();
    this.storeBytesToUpload = new ConcurrentHashMap<>();
    this.storeBytesToRetain = new ConcurrentHashMap<>();
    this.storeBytesToRemove = new ConcurrentHashMap<>();

    this.uploadRate = metricsRegistry.newCounter(GROUP, "upload-rate");

    this.cleanupNs = metricsRegistry.newTimer(GROUP, "cleanup-ns");

    this.avgFileUploadNs = metricsRegistry.newTimer(GROUP,  "avg-file-upload-ns");
    this.avgFileSizeBytes = metricsRegistry.newTimer(GROUP, "avg-file-size-bytes");
  }

  public void initStoreMetrics(Collection<String> storeNames) {
    for (String storeName: storeNames) {
      storeDirDiffNs.putIfAbsent(storeName,
          metricsRegistry.newTimer(GROUP, String.format("%s-dir-diff-ns", storeName)));
      storeUploadNs.putIfAbsent(storeName,
          metricsRegistry.newTimer(GROUP, String.format("%s-upload-ns", storeName)));

      storeFilesToUpload.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-files-to-upload", storeName), 0L));
      storeFilesToRetain.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-files-to-retain", storeName), 0L));
      storeFilesToRemove.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-files-to-remove", storeName), 0L));
      storeSubDirsToUpload.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-sub-dirs-to-upload", storeName), 0L));
      storeSubDirsToRetain.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-sub-dirs-to-retain", storeName), 0L));
      storeSubDirsToRemove.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-sub-dirs-to-remove", storeName), 0L));
      storeBytesToUpload.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-bytes-to-upload", storeName), 0L));
      storeBytesToRetain.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-bytes-to-retain", storeName), 0L));
      storeBytesToRemove.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-bytes-to-remove", storeName), 0L));
    }
  }
}
