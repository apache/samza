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
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;


public class BlobStoreManagerMetrics {
  private static final String GROUP = BlobStoreManagerMetrics.class.getName();
  private final MetricsRegistry metricsRegistry;

  public final Map<String, Gauge<Long>> storeBlobNotFoundError;

  public BlobStoreManagerMetrics(MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;

    storeBlobNotFoundError = new ConcurrentHashMap<>();
  }

  public void initStoreMetrics(Collection<String> storeNames) {
    for (String storeName : storeNames) {
      storeBlobNotFoundError.putIfAbsent(storeName,
          metricsRegistry.newGauge(GROUP, String.format("%s-blob-not-found", storeName), 0L));
    }
  }
}
