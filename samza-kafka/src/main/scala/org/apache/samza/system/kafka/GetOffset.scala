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
import grizzled.slf4j.Logging
import kafka.message.MessageAndOffset

/**
 * Obtain the correct offsets for topics, be it earliest or largest
 * @param default Value to return if no offset has been specified for topic
 * @param autoOffsetResetTopics Topics that have been specified as auto offset
 */
class GetOffset(default: String, autoOffsetResetTopics: Map[String, String] = Map()) extends Logging with Toss {

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

  /**
   *  An offset was provided but may not be valid.  Verify its validity.
   */
  private def useLastCheckpointedOffset(sc: DefaultFetchSimpleConsumer, last: String, tp: TopicAndPartition): Option[Long] = {
    try {
      info("Validating offset %s for topic and partition %s" format (last, tp))

      val messages: FetchResponse = sc.defaultFetch((tp, last.toLong))

      if (messages.hasError) {
        ErrorMapping.maybeThrowException(messages.errorCode(tp.topic, tp.partition))
      }

      info("Able to successfully read from offset %s for topic and partition %s. Using it to instantiate consumer." format (last, tp))

      val messageSet = messages.messageSet(tp.topic, tp.partition)

      if(messageSet.isEmpty) { // No messages have been written since our checkpoint
        info("Got empty response for checkpointed offset, using checkpointed")
        Some(last.toLong)
      } else {
        val nextOffset = messageSet.head.nextOffset
        info("Got next offset %s for %s." format (nextOffset, tp))
        Some(nextOffset)
      }
    } catch {
      case e: OffsetOutOfRangeException =>
        info("An out-of-range Kafka offset (%s) was supplied for topic and partition %s, so falling back to autooffset.reset." format (last, tp))
        None
    }
  }

  /**
   * Using the provided SimpleConsumer, obtain the next offset to read for the specified topic
   * @param sc SimpleConsumer used to query the Kafka Broker
   * @param tp TopicAndPartition we offset for
   * @param lastCheckpointedOffset Null is acceptable. If not null, return the last checkpointed offset, after checking it is valid
   * @return Next offset to read or throw an exception if one has been received via the simple consumer
   */
  def getNextOffset(sc: DefaultFetchSimpleConsumer, tp: TopicAndPartition, lastCheckpointedOffset: String): Long = {
    val offsetRequest = new OffsetRequest(Map(tp -> new PartitionOffsetRequestInfo(getAutoOffset(tp.topic), 1)))
    val offsetResponse = sc.getOffsetsBefore(offsetRequest)
    val partitionOffsetResponse = offsetResponse.partitionErrorAndOffsets.get(tp).getOrElse(toss("Unable to find offset information for %s" format tp))

    ErrorMapping.maybeThrowException(partitionOffsetResponse.error)

    val autoOffset = partitionOffsetResponse.offsets.headOption.getOrElse(toss("Got response, but no offsets defined for %s" format tp))

    val actualOffset = Option(lastCheckpointedOffset) match {
      case Some(last) => useLastCheckpointedOffset(sc, last, tp).getOrElse(autoOffset)
      case None => autoOffset
    }

    info("Final offset to be returned for Topic and Partition %s = %d" format (tp, actualOffset))
    actualOffset
  }
}
