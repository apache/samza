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

package org.apache.samza.system.kafka

import grizzled.slf4j.Logging
import kafka.api.TopicMetadata
import kafka.common.ErrorMapping

/**
 * TopicMetadataCache is used to cache all the topic metadata for Kafka per
 * (system, topic) partition. The cache access is thread safe. Each entry in
 * the cache is refreshed after a specified interval. The cache uses the passed
 * in getTopicInfoFromStore that retrieves the topic metadata from the store (usually zookeeper).
 */
object TopicMetadataCache extends Logging {
  private case class MetadataInfo(var streamMetadata: TopicMetadata, var lastRefreshMs: Long)
  private val topicMetadataMap: scala.collection.mutable.Map[(String, String), MetadataInfo] = new scala.collection.mutable.HashMap[(String, String), MetadataInfo]
  private val lock = new Object

  // used to fetch the topic metadata from the store. Accepts a topic and system
  type FetchTopicMetadataType = (Set[String]) => Map[String, TopicMetadata]

  def getTopicMetadata(topics: Set[String], systemName: String, getTopicInfoFromStore: FetchTopicMetadataType, cacheTimeout: Long = 5000L, getTime: () => Long = { System.currentTimeMillis }): Map[String, TopicMetadata] = {
    lock synchronized {
      val time = getTime()
      val missingTopics = topics.filter(topic => !topicMetadataMap.contains(systemName, topic))
      val topicsWithBadOrExpiredMetadata = (topics -- missingTopics).filter(topic => {
        val metadata = topicMetadataMap(systemName, topic)
        metadata.streamMetadata.errorCode != ErrorMapping.NoError || ((time - metadata.lastRefreshMs) > cacheTimeout)
      })
      val topicsToRefresh = missingTopics ++ topicsWithBadOrExpiredMetadata

      if (topicsToRefresh.size > 0) {
        // Refresh topic information for any missing, expired, or bad topic metadata.
        topicMetadataMap ++= getTopicInfoFromStore(missingTopics ++ topicsWithBadOrExpiredMetadata)
          .map { case (topic, metadata) => ((systemName, topic), MetadataInfo(metadata, getTime())) }
          .toMap
      }

      // Use our new updated cache to return a map of topic -> metadata
      topicMetadataMap
        .filterKeys(topics.map(topic => (systemName, topic)))
        .map {
          case ((systemName, topic), metadata) =>
            (topic, metadata.streamMetadata)
        }.toMap
    }
  }

  def clear {
    topicMetadataMap.clear
  }
}
