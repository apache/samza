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
package org.apache.samza.system;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.util.Clock;


/**
 * Fetches and caches metadata about a set of SSPs. When fetching metadata for a stale SSP, this will also prefetch
 * metadata for all other SSPs specified for prefetching which are stale and in the same system.
 */
public class SSPMetadataCache {
  private final SystemAdmins systemAdmins;
  private final Duration cacheTTL;
  private final Clock clock;
  private final Set<SystemStreamPartition> sspsToPrefetch;

  private final Object metadataRefreshLock;
  private final ConcurrentHashMap<SystemStreamPartition, CacheEntry> cache;

  public SSPMetadataCache(SystemAdmins systemAdmins, Duration cacheTTL, Clock clock,
      Set<SystemStreamPartition> sspsToPrefetch) {
    this.systemAdmins = systemAdmins;
    this.cacheTTL = cacheTTL;
    this.clock = clock;
    this.sspsToPrefetch = sspsToPrefetch;

    this.metadataRefreshLock = new Object();
    this.cache = new ConcurrentHashMap<>();
  }

  /**
   * Gets the metadata for an SSP. This will return a cached value if it is fresh enough. Otherwise, it will fetch the
   * metadata from a source-of-truth.
   * If the metadata for the SSP needs to be fetched, then this will also prefetch and cache the metadata for any stale
   * {@link #sspsToPrefetch} that can be included in the same fetch call (e.g. same system).
   *
   * @param ssp SSP for which to get metadata
   * @return metadata for the SSP; null if the source-of-truth returned no metadata
   * @throws RuntimeException if there was an error in fetching metadata
   */
  public SystemStreamMetadata.SystemStreamPartitionMetadata getMetadata(SystemStreamPartition ssp) {
    maybeRefreshMetadata(ssp);
    CacheEntry cacheEntry = cache.get(ssp);
    /*
     * cacheEntry itself should not be null once the refresh is done, but check anyways to be safe.
     * The metadata inside a non-null cacheEntry might still be null, so this will return null in that case.
     */
    return cacheEntry == null ? null : cacheEntry.getMetadata();
  }

  private void maybeRefreshMetadata(SystemStreamPartition requestedSSP) {
    synchronized (this.metadataRefreshLock) {
      Instant refreshRequestedAt = Instant.ofEpochMilli(this.clock.currentTimeMillis());
      if (shouldRefresh(requestedSSP, refreshRequestedAt)) {
        String system = requestedSSP.getSystem();
        Set<SystemStreamPartition> sspsToFetchFor = new HashSet<>();
        sspsToFetchFor.add(requestedSSP);
        for (SystemStreamPartition sspToPrefetch : this.sspsToPrefetch) {
          if (system.equals(sspToPrefetch.getSystem()) && shouldRefresh(sspToPrefetch, refreshRequestedAt)) {
            sspsToFetchFor.add(sspToPrefetch);
          }
        }
        SystemAdmin systemAdmin = this.systemAdmins.getSystemAdmin(system);
        Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> fetchedMetadata =
            systemAdmin.getSSPMetadata(sspsToFetchFor);
        Instant updatedAt = Instant.ofEpochMilli(this.clock.currentTimeMillis());
        // we want to add an entry even if there was no metadata, so iterate over sspsToFetchFor
        sspsToFetchFor.forEach(ssp -> this.cache.put(ssp, new CacheEntry(fetchedMetadata.get(ssp), updatedAt)));
      }
    }
  }

  private boolean shouldRefresh(SystemStreamPartition ssp, Instant now) {
    CacheEntry cacheEntry = cache.get(ssp);
    if (cacheEntry == null) {
      return true;
    } else {
      Instant isFreshUntil = cacheEntry.getLastUpdatedAt().plus(cacheTTL);
      return now.isAfter(isFreshUntil);
    }
  }

  private static class CacheEntry {
    /**
     * Nullable so that we can cache that there was no metadata for the last fetch.
     */
    private final SystemStreamMetadata.SystemStreamPartitionMetadata metadata;
    private final Instant lastUpdatedAt;

    private CacheEntry(SystemStreamMetadata.SystemStreamPartitionMetadata metadata, Instant lastUpdatedAt) {
      this.metadata = metadata;
      this.lastUpdatedAt = lastUpdatedAt;
    }

    private SystemStreamMetadata.SystemStreamPartitionMetadata getMetadata() {
      return metadata;
    }

    private Instant getLastUpdatedAt() {
      return lastUpdatedAt;
    }
  }
}
