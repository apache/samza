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

package org.apache.samza.system

import java.util
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

import com.google.common.collect.{ImmutableMap, ImmutableSet}
import org.apache.samza.SamzaException
import org.apache.samza.util.{Clock, Logging, SystemClock}

import scala.collection.JavaConverters._

/**
 * Caches requests to SystemAdmin.getSystemStreamMetadata for a short while (by default
 * 5 seconds), so that we can make many metadata requests in quick succession without
 * hammering the actual systems. This is useful for example during task startup, when
 * each task independently fetches the offsets for own partition.
 */
class StreamMetadataCache (
    /** System implementations from which the actual metadata is loaded on cache miss */
    systemAdmins: SystemAdmins,

    /** Maximum age (in milliseconds) of a cache entry */
    val cacheTTLms: Int = 5000,

    /** Clock used for determining expiry (for mocking in tests) */
    clock: Clock = SystemClock.instance) extends Logging {

  /**
    * @param lastRefreshMs last time that the entry was updated
    * @tparam T type of data in entry
    */
  private case class CacheEntry[T](data: T, lastRefreshMs: Long)

  private val fullMetadataCache = new ConcurrentHashMap[SystemStream, CacheEntry[SystemStreamMetadata]]()

  /**
    * Caches newest offset data.
    * The CacheEntry data is Optional so we can also cache if there is "no newest offset" (e.g. topic is empty). This
    * allows us to keep an entry in the cache for prefetching in the future.
    * Using a regular HashMap here instead of ConcurrentHashMap because updates already need to be protected by a lock.
    */
  private val newestOffsetCache = new util.HashMap[SystemStreamPartition, CacheEntry[Optional[String]]]()

  /**
   * Returns metadata about each of the given streams (such as first offset, newest
   * offset, etc). If the metadata isn't in the cache, it is retrieved from the systems
   * using the given SystemAdmins.
   *
   * @param streams Set of SystemStreams for which the metadata is requested
   * @param partitionsMetadataOnly Flag to indicate that only partition count metadata should be fetched/refreshed
   */
  def getStreamMetadata(
    streams: Set[SystemStream],
    partitionsMetadataOnly: Boolean = false): Map[SystemStream, SystemStreamMetadata] = {
    val time = clock.currentTimeMillis
    val cacheHits = streams.flatMap(stream => getFromCache(fullMetadataCache, stream, time)).toMap

    val cacheMisses = (streams -- cacheHits.keySet)
      .groupBy[String](_.getSystem)
      .flatMap {
        case (systemName, systemStreams) =>
          val systemAdmin = systemAdmins.getSystemAdmin(systemName)
          val streamToMetadata = if (partitionsMetadataOnly && systemAdmin.isInstanceOf[ExtendedSystemAdmin]) {
            systemAdmin.asInstanceOf[ExtendedSystemAdmin].getSystemStreamPartitionCounts(systemStreams.map(_.getStream).asJava, cacheTTLms)
          } else {
            systemAdmin.getSystemStreamMetadata(systemStreams.map(_.getStream).asJava)
          }
          streamToMetadata.asScala.map {
            case (streamName, metadata) => (new SystemStream(systemName, streamName) -> metadata)
          }
      }

    val allResults = cacheHits ++ cacheMisses
    val missing = streams.filter(stream => allResults.getOrElse(stream, null) == null)
    if (!missing.isEmpty) {
      throw new SamzaException("Cannot get metadata for unknown streams: " + missing.mkString(", "))
    }
    if (!partitionsMetadataOnly) {
      cacheMisses.foreach { case (stream, metadata) => fullMetadataCache.put(stream, CacheEntry(metadata, time))
      }
    }
    allResults
  }

  /**
   * Returns metadata about the given streams. If the metadata isn't in the cache, it is retrieved from the systems
   * using the given SystemAdmins.
   *
   * @param stream SystemStreams for which the metadata is requested
   * @param partitionsMetadataOnly Flag to indicate that only partition count metadata should be fetched/refreshed
   */
  def getSystemStreamMetadata(stream: SystemStream, partitionsMetadataOnly: Boolean): SystemStreamMetadata = {
    getStreamMetadata(Set(stream), partitionsMetadataOnly).get(stream).orNull
  }

  /**
    * Returns a tuple (key, data in the CacheEntry corresponding to key in the cache).
    * If there is no CacheEntry, or the CacheEntry is stale, then this will return None.
    */
  private def getFromCache[K, V] (cache: util.Map[K, CacheEntry[V]], key: K, now: Long): Option[(K, V)] = {
    if (cache.containsKey(key)) {
      val cacheEntry = cache.get(key)
      if (!isStale(now, cacheEntry.lastRefreshMs)) {
        return Some(key -> cacheEntry.data)
      }
    }
    None
  }

  /**
    * Get the newest offset for the systemStreamPartition. This returns null if there is no newest offset (e.g. stream
    * is empty).
    * Returns a cached value if it is fresh enough, even if the cached value has no newest offset data. Otherwise, call
    * the admin to get the newest offset.
    * As an optimization, this will also prefetch the newest offsets for other systemStreamPartitions that have been
    * fetched in previous calls to getNewestOffset.
    */
  def getNewestOffset(systemStreamPartition: SystemStreamPartition): String = {
    // needs synchronization so that other threads don't come in a try to also do a fetch
    newestOffsetCache synchronized {
      val now = clock.currentTimeMillis
      getFromCache(newestOffsetCache, systemStreamPartition, now) match {
        // has a fresh enough entry; even if it does not have data, don't do another fetch yet
        case Some((key, newestOffset)) => newestOffset.orElse(null)
        // no entry or stale entry; do some fetching
        case None => fetchNewestOffsetWithPrefetching(systemStreamPartition, now)
      }
    }
  }

  /**
    * Fetch the newest offset for requestedSSP. Returns null if there is no newest offset.
    * Also prefetch the newest offsets for any other stale SSPs which had the same system as requestedSSP.
    */
  private def fetchNewestOffsetWithPrefetching(requestedSSP: SystemStreamPartition, now: Long): String = {
    val systemToFetch = requestedSSP.getSystem
    /*
     * If an ssp is in the newestOffsetCache, then we have fetched it before, so include it in the metadata fetch
     * request now if it is stale. Also make sure it has the same system, since we will only use the admin for the
     * system of the requestedSSP.
     */
    val sspsToFetchBuilder : ImmutableSet.Builder[SystemStreamPartition] = new ImmutableSet.Builder()
    sspsToFetchBuilder.add(requestedSSP)
    for (entry : util.Map.Entry[SystemStreamPartition, CacheEntry[Optional[String]]] <- newestOffsetCache.entrySet.asScala) {
      val sspToMaybeFetch = entry.getKey
      if (systemToFetch.equals(sspToMaybeFetch.getSystem) && isStale(now, entry.getValue.lastRefreshMs)) {
        sspsToFetchBuilder.add(sspToMaybeFetch)
      }
    }
    val sspsToFetch = sspsToFetchBuilder.build()
    val newestOffsets: util.Map[SystemStreamPartition, String] =
      fetchNewestOffsetsWithSystemAdmin(sspsToFetch, systemAdmins.getSystemAdmin(systemToFetch))
    // want to put an entry in regardless of if we got data from newestOffsets, so iterate over sspsToFetch
    for (sspToFetch : SystemStreamPartition <- sspsToFetch.asScala) {
      val cacheEntryData = Optional.ofNullable(newestOffsets.get(sspToFetch))
      newestOffsetCache.put(sspToFetch, CacheEntry(cacheEntryData, now))
    }
    newestOffsets.get(requestedSSP)
  }

  private def fetchNewestOffsetsWithSystemAdmin(sspsToFetch: util.Set[SystemStreamPartition],
                                                systemAdmin: SystemAdmin): util.Map[SystemStreamPartition, String] = {
    systemAdmin match {
      case extendedSystemAdmin: ExtendedSystemAdmin =>
        /*
         * ExtendedSystemAdmin has a way to fetch the newest offsets for specific SSPs instead of fetching the
         * newest and oldest offsets for all SSPs. Use it if we can.
         */
        extendedSystemAdmin.getNewestOffsets(sspsToFetch)
      case fallbackSystemAdmin =>
        val streamsToFetchBuilder : ImmutableSet.Builder[String] = new ImmutableSet.Builder()
        for (sspToFetch : SystemStreamPartition <- sspsToFetch.asScala) {
          streamsToFetchBuilder.add(sspToFetch.getStream)
        }
        val streamsToFetch = streamsToFetchBuilder.build()
        val streamToMetadata : util.Map[String, SystemStreamMetadata] =
          fallbackSystemAdmin.getSystemStreamMetadata(streamsToFetch)
        // TODO can we add this to the fullMetadataCache as well?
        val sspToNewestOffsetBuilder : ImmutableMap.Builder[SystemStreamPartition, String] = new ImmutableMap.Builder()
        for (sspToFetch : SystemStreamPartition <- sspsToFetch.asScala) {
          val systemStreamMetadata = streamToMetadata.get(sspToFetch.getStream)
          if (systemStreamMetadata != null) {
            val sspMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata
            val partition = sspToFetch.getPartition
            val partitionMetadata = sspMetadata.get(partition)
            if (partitionMetadata != null && partitionMetadata.getNewestOffset != null) {
              sspToNewestOffsetBuilder.put(sspToFetch, partitionMetadata.getNewestOffset)
            }
          }
        }
        sspToNewestOffsetBuilder.build()
    }
  }

  private def isStale(now: Long, lastRefreshedAt: Long): Boolean = {
    now - lastRefreshedAt > cacheTTLms
  }
}
