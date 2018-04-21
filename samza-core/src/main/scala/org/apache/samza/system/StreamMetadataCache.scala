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

import org.apache.samza.SamzaException
import org.apache.samza.util.{Clock, Logging, SystemClock}

import scala.collection.JavaConverters._
import scala.collection.{MapLike, mutable}

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

  private var fullMetadataCache = Map[SystemStream, CacheEntry[SystemStreamMetadata]]()
  private val lock = new Object

  /**
    * Caches newest offset data.
    * If there is a CacheEntry with Some data, and the TTL has not passed, then use the data and do not fetch.
    * If there is a CacheEntry with None data, and the TTL has not passed, then use the empty entry and do not fetch.
    * If there is no CacheEntry, then fetch the metadata and fill in the cache.
    *
    * Using a mutable.Map here instead of immutable.Map because this will get updated relatively often, so we do not
    * need to create new maps for each update. This does need to be protected by a lock for thread safety.
    */
  private var newestOffsetCache = mutable.Map[SystemStreamPartition, CacheEntry[Option[String]]]()

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
      cacheMisses.foreach { case (stream, metadata) => addToFullMetadataCache(stream, metadata, time) }
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
    * Using a MapLike argument in order to work with both immutable.Map and mutable.Map to get an entry.
    */
  private def getFromCache[K, V, M <: MapLike[K, CacheEntry[V], M] with scala.collection.Map[K, CacheEntry[V]]]
    (cache: MapLike[K, CacheEntry[V], M],
     key: K,
     now: Long): Option[(K, V)] = {
    cache.get(key) match {
      case Some(CacheEntry(data, lastRefresh)) => if (isStale(now, lastRefresh)) None else Some(key -> data)
      case None => None
    }
  }

  private def addToFullMetadataCache(systemStream: SystemStream, metadata: SystemStreamMetadata, now: Long) {
    lock synchronized {
      fullMetadataCache += systemStream -> CacheEntry(metadata, now)
    }
  }

  /**
    * Get the newest offset for the systemStreamPartition. This returns null if there is no newest offset.
    * Returns a cached value if it is fresh enough, even if the cached value has no newest offset data (e.g. stream is
    * empty). Otherwise, call the admin to get the newest offset.
    * As an optimization, this will also prefetch the newest offsets for other systemStreamPartitions that have been
    * fetched in previous calls to getNewestOffset.
    * @param maxRetries Max number of retries to use when fetching the newest offset for an individual
    *                   systemStreamPartition. This is also the number of retries used when doing prefetching for other
    *                   systemStreamPartitions.
    */
  def getNewestOffset(systemStreamPartition: SystemStreamPartition, maxRetries: Integer): String = {
    // needs synchronization so that other threads don't come in a try to also do a fetch
    newestOffsetCache synchronized {
      val now = clock.currentTimeMillis
      getFromCache(newestOffsetCache, systemStreamPartition, now) match {
        // has a fresh enough entry with data, return it
        case Some((key, Some(offset))) => offset
        // has a fresh enough entry which has no data, still don't want to fetch again yet
        case Some((key, None)) => null
        // no entry or stale entry; do some fetching
        case None => fetchNewestOffsetWithPrefetching(systemStreamPartition, maxRetries, now)
      }
    }
  }

  /**
    * Fetch the newest offset for requestedSystemStreamPartition.
    * Also prefetch the newest offsets for any other stale SSPs which had the same system as
    * requestedSystemStreamPartition.
    */
  private def fetchNewestOffsetWithPrefetching(requestedSystemStreamPartition: SystemStreamPartition,
                                               maxRetries: Integer,
                                               now: Long): String = {
    val systemToFetch = requestedSystemStreamPartition.getSystem
    /*
     * If a systemStreamPartition is in the newestOffsetCache, then we have fetched it before, so include it in
     * the metadata fetch request now if it is stale. Also make sure it has the same system, since we will only use the
     * admin for the system of the requestedSystemStreamPartition.
     */
    var systemStreamPartitionsToFetch = newestOffsetCache
      .filter({case (ssp, cacheEntry) => systemToFetch.equals(ssp.getSystem) && isStale(now, cacheEntry.lastRefreshMs)})
      .keySet.toSet
    if (!systemStreamPartitionsToFetch.contains(requestedSystemStreamPartition)) {
      systemStreamPartitionsToFetch += requestedSystemStreamPartition
    }
    val newestOffsets: Map[SystemStreamPartition, String] = systemAdmins.getSystemAdmin(systemToFetch) match {
      case extendedSystemAdmin: ExtendedSystemAdmin =>
        /*
         * ExtendedSystemAdmin has a way to fetch the newest offsets for specific SSPs instead of fetching the
         * newest and oldest offsets for all SSPs. Use it if we can.
         */
        extendedSystemAdmin.getNewestOffsets(systemStreamPartitionsToFetch.asJava, maxRetries).asScala.toMap
      case systemAdmin => fetchNewestOffsetsForSystemAdmin(systemStreamPartitionsToFetch, systemAdmin)
    }
    addNewestOffsetsToCache(systemStreamPartitionsToFetch, newestOffsets, now)
    newestOffsets.get(requestedSystemStreamPartition) match {
      case Some(offset) => offset
      case None => null
    }
  }

  /**
    * Add the newest offsets data to the cache.
    * The systemStreamPartitionsToFetch are needed because newestOffsets might not have an entry for all SSPs.
    */
  def addNewestOffsetsToCache(systemStreamPartitionsToFetch: Set[SystemStreamPartition],
                              newestOffsets: Map[SystemStreamPartition, String],
                              time: Long): Unit = {
    systemStreamPartitionsToFetch.foreach(systemStreamPartition => {
      // want to put an entry in regardless of if we got data from newestOffsets
      newestOffsetCache.update(systemStreamPartition, CacheEntry(newestOffsets.get(systemStreamPartition), time))
    })
  }

  def fetchNewestOffsetsForSystemAdmin(systemStreamPartitionsToFetch: Set[SystemStreamPartition],
                                       systemAdmin: SystemAdmin): Map[SystemStreamPartition, String] = {
    val streamsToFetch = systemStreamPartitionsToFetch
      .map(systemStreamPartitionToFetch => systemStreamPartitionToFetch.getStream)
    val streamToMetadata = systemAdmin.getSystemStreamMetadata(streamsToFetch.asJava).asScala.toMap
    systemStreamPartitionsToFetch.flatMap(systemStreamPartition =>
      streamToMetadata.get(systemStreamPartition.getStream) match {
        case Some(systemStreamMetadata) =>
          val systemStreamPartitionMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata
          val partition = systemStreamPartition.getPartition
          val partitionMetadata = systemStreamPartitionMetadata.get(partition)
          if (partitionMetadata != null && partitionMetadata.getNewestOffset != null) {
            Some(systemStreamPartition -> partitionMetadata.getNewestOffset)
          } else {
            None
          }
        case None => None
      }).toMap
  }

  private def isStale(now: Long, lastRefreshedAt: Long): Boolean = {
    now - lastRefreshedAt > cacheTTLms
  }
}
