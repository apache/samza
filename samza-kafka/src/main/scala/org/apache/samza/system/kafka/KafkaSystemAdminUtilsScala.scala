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

import java.util
import java.util.Properties

import kafka.admin.{AdminClient, AdminUtils}
import kafka.utils.{Logging, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.samza.config.ApplicationConfig.ApplicationMode
import org.apache.samza.config.{ApplicationConfig, Config, KafkaConfig, StreamConfig}
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.{StreamSpec, SystemStreamMetadata, SystemStreamPartition}
import org.apache.samza.util.ExponentialSleepStrategy
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * A helper class that is used to construct the changelog stream specific information
  *
  * @param replicationFactor The number of replicas for the changelog stream
  * @param kafkaProps The kafka specific properties that need to be used for changelog stream creation
  */
case class ChangelogInfo(var replicationFactor: Int, var kafkaProps: Properties)


// TODO move to org.apache.kafka.clients.admin.AdminClien from the kafka.admin.AdminClient
object KafkaSystemAdminUtilsScala extends Logging {

  val CLEAR_STREAM_RETRIES = 3
  val CREATE_STREAM_RETRIES = 10

  /**
    * @inheritdoc
    *
    * Delete a stream in Kafka. Deleting topics works only when the broker is configured with "delete.topic.enable=true".
    * Otherwise it's a no-op.
    */
  def clearStream(spec: StreamSpec, connectZk: java.util.function.Supplier[ZkUtils]): Unit = {
    info("Deleting topic %s for system %s" format(spec.getPhysicalName, spec.getSystemName))
    val kSpec = KafkaStreamSpec.fromSpec(spec)
    var retries = CLEAR_STREAM_RETRIES
    new ExponentialSleepStrategy().run(
      loop => {
        val zkClient = connectZk.get()
        try {
          AdminUtils.deleteTopic(
            zkClient,
            kSpec.getPhysicalName)
        } finally {
          zkClient.close
        }

        loop.done
      },

      (exception, loop) => {
        if (retries > 0) {
          warn("Exception while trying to delete topic %s. Retrying." format (spec.getPhysicalName), exception)
          retries -= 1
        } else {
          warn("Fail to delete topic %s." format (spec.getPhysicalName), exception)
          loop.done
          throw exception
        }
      })
  }


  def createStream(kSpec: KafkaStreamSpec, connectZk: java.util.function.Supplier[ZkUtils]): Boolean = {
    info("Creating topic %s for system %s" format(kSpec.getPhysicalName, kSpec.getSystemName))
    var streamCreated = false
    var retries = CREATE_STREAM_RETRIES

    new ExponentialSleepStrategy(initialDelayMs = 500).run(
      loop => {
        val zkClient = connectZk.get()
        try {
          AdminUtils.createTopic(
            zkClient,
            kSpec.getPhysicalName,
            kSpec.getPartitionCount,
            kSpec.getReplicationFactor,
            kSpec.getProperties)
        } finally {
          zkClient.close
        }

        streamCreated = true
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: TopicExistsException =>
            streamCreated = false
            loop.done
          case e: Exception =>
            if (retries > 0) {
              warn("Failed to create topic %s. Retrying." format (kSpec.getPhysicalName), exception)
              retries -= 1
            } else {
              error("Failed to create topic %s. Bailing out." format (kSpec.getPhysicalName), exception)
              throw exception
            }
        }
      })

    streamCreated
  }

  /**
    * A helper method that takes oldest, newest, and upcoming offsets for each
    * system stream partition, and creates a single map from stream name to
    * SystemStreamMetadata.
    */
  def assembleMetadata(oldestOffsets: Map[SystemStreamPartition, String], newestOffsets: Map[SystemStreamPartition, String], upcomingOffsets: Map[SystemStreamPartition, String]): Map[String, SystemStreamMetadata] = {
    val allMetadata = (oldestOffsets.keySet ++ newestOffsets.keySet ++ upcomingOffsets.keySet)
      .groupBy(_.getStream)
      .map {
        case (streamName, systemStreamPartitions) =>
          val streamPartitionMetadata = systemStreamPartitions
            .map(systemStreamPartition => {
              val partitionMetadata = new SystemStreamPartitionMetadata(
                // If the topic/partition is empty then oldest and newest will
                // be stripped of their offsets, so default to null.
                oldestOffsets.getOrElse(systemStreamPartition, null),
                newestOffsets.getOrElse(systemStreamPartition, null),
                upcomingOffsets(systemStreamPartition))
              (systemStreamPartition.getPartition, partitionMetadata)
            })
            .toMap
          val streamMetadata = new SystemStreamMetadata(streamName, streamPartitionMetadata.asJava)
          (streamName, streamMetadata)
      }
      .toMap

    // This is typically printed downstream and it can be spammy, so debug level here.
    debug("Got metadata: %s" format allMetadata)

    allMetadata
  }

  def getCoordinatorTopicProperties(config: KafkaConfig) = {
    val segmentBytes = config.getCoordinatorSegmentBytes
    (new Properties /: Map(
      "cleanup.policy" -> "compact",
      "segment.bytes" -> segmentBytes)) { case (props, (k, v)) => props.put(k, v); props }
  }

  def getIntermediateStreamProperties(config: Config): Map[String, Properties] = {
    val appConfig = new ApplicationConfig(config)
    if (appConfig.getAppMode == ApplicationMode.BATCH) {
      val streamConfig = new StreamConfig(config)
      streamConfig.getStreamIds().filter(streamConfig.getIsIntermediateStream(_)).map(streamId => {
        val properties = new Properties()
        properties.putAll(streamConfig.getStreamProperties(streamId))
        properties.putIfAbsent("retention.ms", String.valueOf(KafkaConfig.DEFAULT_RETENTION_MS_FOR_BATCH))
        (streamId, properties)
      }).toMap
    } else {
      Map()
    }
  }

  def deleteMessages(adminClient : AdminClient, offsets: util.Map[SystemStreamPartition, String]) = {
    val nextOffsets = offsets.asScala.toSeq.map { case (systemStreamPartition, offset) =>
      (new TopicPartition(systemStreamPartition.getStream, systemStreamPartition.getPartition.getPartitionId), offset.toLong + 1)
    }.toMap
    adminClient.deleteRecordsBefore(nextOffsets);
  }
}
