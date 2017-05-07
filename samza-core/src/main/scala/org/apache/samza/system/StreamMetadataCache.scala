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

import org.apache.samza.config.Config
import org.apache.samza.util.{Util, Logging, Clock, SystemClock}
import org.apache.samza.SamzaException
import scala.collection.JavaConverters._
import org.apache.samza.config.SystemConfig.Config2System

object StreamMetadataCache {
  def apply(cacheTtlMs: Int = 5000, config: Config): StreamMetadataCache = {
    val systemNames = config.getSystemNames.toSet
    // Map the name of each system to the corresponding SystemAdmin
    val systemAdmins = systemNames.map(systemName => {
      val systemFactoryClassName = config
        .getSystemFactory(systemName)
        .getOrElse(throw new SamzaException("A stream uses system %s, which is missing from the configuration." format systemName))
      val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
      systemName -> systemFactory.getAdmin(systemName, config)
    }).toMap

    new StreamMetadataCache(systemAdmins, cacheTtlMs, SystemClock.instance)
  }
}
/**
 * Caches requests to SystemAdmin.getSystemStreamMetadata for a short while (by default
 * 5 seconds), so that we can make many metadata requests in quick succession without
 * hammering the actual systems. This is useful for example during task startup, when
 * each task independently fetches the offsets for own partition.
 */
class StreamMetadataCache (
    /** System implementations from which the actual metadata is loaded on cache miss */
    systemAdmins: Map[String, SystemAdmin],

    /** Maximum age (in milliseconds) of a cache entry */
    val cacheTTLms: Int = 5000,

    /** Clock used for determining expiry (for mocking in tests) */
    clock: Clock = SystemClock.instance) extends Logging {

  private case class CacheEntry(metadata: SystemStreamMetadata, lastRefreshMs: Long)
  private var cache = Map[SystemStream, CacheEntry]()
  private val lock = new Object
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
    val cacheHits = streams.flatMap(stream => getFromCache(stream, time)).toMap

    val cacheMisses = (streams -- cacheHits.keySet)
      .groupBy[String](_.getSystem)
      .flatMap {
        case (systemName, systemStreams) =>
          val systemAdmin = systemAdmins
            .getOrElse(systemName, throw new SamzaException("Cannot get metadata for unknown system: %s" format systemName))
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
      cacheMisses.foreach { case (stream, metadata) => addToCache(stream, metadata, time) }
    }
    allResults
  }

  private def getFromCache(stream: SystemStream, now: Long) = {
    cache.get(stream) match {
      case Some(CacheEntry(metadata, lastRefresh)) =>
        if (now - lastRefresh > cacheTTLms) None else Some(stream -> metadata)
      case None => None
    }
  }

  private def addToCache(systemStream: SystemStream, metadata: SystemStreamMetadata, now: Long) {
    lock synchronized {
      cache += systemStream -> CacheEntry(metadata, now)
    }
  }
}
