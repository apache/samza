/*
 *
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
 *
 */

package org.apache.samza.system.kafka

import kafka.common.{ OffsetOutOfRangeException, ErrorMapping }
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo
import org.apache.samza.util.Logging
import kafka.message.MessageAndOffset

/**
 * GetOffset validates offsets for topic partitions, and manages fetching new
 * offsets for topics using Kafka's auto.offset.reset configuration.
 */
class GetOffset(
  /**
   * The default auto.offset.reset to use if a topic is not overridden in
   * autoOffsetResetTopics. Any value other than "earliest" or "latest" will
   * result in an exception when getRestOffset is called.
   */
  default: String,

  /**
   * Topic-level overrides for auto.offset.reset. Any value other than
   * "earliest" or "latest" will result in an exception when getRestOffset is
   * called.
   */
  autoOffsetResetTopics: Map[String, String] = Map()) extends Logging with Toss {

  /**
   * Checks if an offset is valid for a given topic/partition. Validity is
   * defined as an offset that returns a readable non-empty message set with
   * no exceptions.
   */
  def isValidOffset(consumer: DefaultFetchSimpleConsumer, topicAndPartition: TopicAndPartition, offset: String) = {
    info("Validating offset %s for topic and partition %s" format (offset, topicAndPartition))

    try {
      val messages = consumer.defaultFetch((topicAndPartition, offset.toLong))

      if (messages.hasError) {
        ErrorMapping.maybeThrowException(messages.errorCode(topicAndPartition.topic, topicAndPartition.partition))
      }

      info("Able to successfully read from offset %s for topic and partition %s. Using it to instantiate consumer." format (offset, topicAndPartition))

      true
    } catch {
      case e: OffsetOutOfRangeException => false
    }
  }

  /**
   * Uses a topic's auto.offset.reset setting (defined via the
   * autoOffsetResetTopics map in the constructor) to fetch either the
   * earliest or latest offset. If neither earliest or latest is defined for
   * the topic in question, the default supplied in the constructor will be
   * used.
   */
  def getResetOffset(consumer: DefaultFetchSimpleConsumer, topicAndPartition: TopicAndPartition) = {
    val offsetRequest = new OffsetRequest(Map(topicAndPartition -> new PartitionOffsetRequestInfo(getAutoOffset(topicAndPartition.topic), 1)))
    val offsetResponse = consumer.getOffsetsBefore(offsetRequest)
    val partitionOffsetResponse = offsetResponse
      .partitionErrorAndOffsets
      .get(topicAndPartition)
      .getOrElse(toss("Unable to find offset information for %s" format topicAndPartition))

    ErrorMapping.maybeThrowException(partitionOffsetResponse.error)

    partitionOffsetResponse
      .offsets
      .headOption
      .getOrElse(toss("Got response, but no offsets defined for %s" format topicAndPartition))
  }

  /**
   * Returns either the earliest or latest setting (a Kafka constant) for a
   * given topic using the autoOffsetResetTopics map defined in the
   * constructor. If the topic is not defined in autoOffsetResetTopics, the
   * default value supplied in the constructor will be used. This is used in
   * conjunction with getResetOffset to fetch either the earliest or latest
   * offset for a topic.
   */
  private def getAutoOffset(topic: String): Long = {
    info("Checking if auto.offset.reset is defined for topic %s" format (topic))
    autoOffsetResetTopics.getOrElse(topic, default) match {
      case OffsetRequest.LargestTimeString =>
        info("Got reset of type %s." format OffsetRequest.LargestTimeString)
        OffsetRequest.LatestTime
      case OffsetRequest.SmallestTimeString =>
        info("Got reset of type %s." format OffsetRequest.SmallestTimeString)
        OffsetRequest.EarliestTime
      case other => toss("Can't get offset value for topic %s due to invalid value: %s" format (topic, other))
    }
  }
}
