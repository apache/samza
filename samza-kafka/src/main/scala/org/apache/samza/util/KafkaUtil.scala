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

package org.apache.samza.util

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.PartitionInfo
import org.apache.samza.config.Config
import org.apache.samza.config.ConfigException
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.system.OutgoingMessageEnvelope
import kafka.common.{TopicExistsException, ErrorMapping, ReplicaNotAvailableException}
import org.apache.samza.system.kafka.TopicMetadataCache

object KafkaUtil extends Logging {
  /**
   * Version number to track the format of the checkpoint log
   */
  val CHECKPOINT_LOG_VERSION_NUMBER = 1
  val counter = new AtomicLong(0)

  def getClientId(id: String, config: Config): String = getClientId(
    id,
    config.getName.getOrElse(throw new ConfigException("Missing job name.")),
    config.getJobId.getOrElse("1"))

  def getClientId(id: String, jobName: String, jobId: String): String =
    "%s-%s-%s-%s-%s" format
      (id.replaceAll("[^A-Za-z0-9]", "_"),
        jobName.replaceAll("[^A-Za-z0-9]", "_"),
        jobId.replaceAll("[^A-Za-z0-9]", "_"),
        System.currentTimeMillis,
        counter.getAndIncrement)

  private def abs(n: Int) = if (n == Integer.MIN_VALUE) 0 else math.abs(n)

  def getIntegerPartitionKey(envelope: OutgoingMessageEnvelope, partitions: java.util.List[PartitionInfo]): Integer = {
    val numPartitions = partitions.size
    abs(envelope.getPartitionKey.hashCode()) % numPartitions
  }

  def getCheckpointTopic(jobName: String, jobId: String) =
    "__samza_checkpoint_ver_%d_for_%s_%s" format (CHECKPOINT_LOG_VERSION_NUMBER, jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))

  /**
   * Exactly the same as Kafka's ErrorMapping.maybeThrowException
   * implementation, except suppresses ReplicaNotAvailableException exceptions.
   * According to the Kafka
   * <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol">protocol
   * docs</a>, ReplicaNotAvailableException can be safely ignored.
   */
  def maybeThrowException(code: Short) {
    try {
      ErrorMapping.maybeThrowException(code)
    } catch {
      case e: ReplicaNotAvailableException =>
        debug("Got ReplicaNotAvailableException, but ignoring since it's safe to do so.")
    }
  }

  /**
   * Checks if a Kafka errorCode is "bad" or not. "Bad" is defined as any
   * errorCode that's not NoError and also not ReplicaNotAvailableCode.
   */
  def isBadErrorCode(code: Short) = {
    code != ErrorMapping.NoError && code != ErrorMapping.ReplicaNotAvailableCode
  }
}

class KafkaUtil(val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                val connectZk: () => ZkClient) extends Logging {
  /**
   * Common code for creating a topic in Kafka
   *
   * @param topicName Name of the topic to be created
   * @param partitionCount  Number of partitions in the topic
   * @param replicationFactor Number of replicas for the topic
   * @param topicProperties Any topic related properties
   */
  def createTopic(topicName: String, partitionCount: Int, replicationFactor: Int, topicProperties: Properties = new Properties) {
    info("Attempting to create topic %s." format topicName)
    retryBackoff.run(
      loop => {
        val zkClient = connectZk()
        try {
          AdminUtils.createTopic(
            zkClient,
            topicName,
            partitionCount,
            replicationFactor,
            topicProperties)
        } finally {
          zkClient.close
        }

        info("Created topic %s." format topicName)
        loop.done
      },

      (exception, loop) => {
        exception match {
          case tee: TopicExistsException =>
            info("Topic %s already exists." format topicName)
            loop.done
          case e: Exception =>
            warn("Failed to create topic %s: %s. Retrying." format(topicName, e))
            debug("Exception detail:", e)
        }
      }
    )
  }

  /**
   * Common code to validate partition count in a topic
   *
   * @param topicName Name of the topic to be validated
   * @param systemName  Kafka system to use
   * @param metadataStore Topic Metadata store
   * @param expectedPartitionCount  Expected number of partitions
   */
  def validateTopicPartitionCount(topicName: String,
                                  systemName: String,
                                  metadataStore: TopicMetadataStore,
                                  expectedPartitionCount: Int) {
    info("Validating topic %s. Expecting partition count: %d" format (topicName, expectedPartitionCount))
    retryBackoff.run(
      loop => {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(Set(topicName), systemName, metadataStore.getTopicInfo)
        val topicMetadata = topicMetadataMap(topicName)
        KafkaUtil.maybeThrowException(topicMetadata.errorCode)

        val partitionCount = topicMetadata.partitionsMetadata.length
        if (partitionCount != expectedPartitionCount) {
          throw new KafkaUtilException("Validation failed for topic %s because partition count %s did not " +
            "match expected partition count of %d." format(topicName, partitionCount, expectedPartitionCount))
        }

        info("Successfully validated topic %s." format topicName)
        loop.done
      },

      (exception, loop) => {
        exception match {
          case e: KafkaUtilException => throw e
          case e: Exception =>
            warn("While trying to validate topic %s: %s. Retrying." format(topicName, e))
            debug("Exception detail:", e)
        }
      }
    )
  }

  /**
   * Code to verify that a topic exists
   *
   * @param topicName Name of the topic
   * @return If exists, it returns true. Otherwise, false.
   */
  def topicExists(topicName: String): Boolean = {
    val zkClient = connectZk()
    try {
      AdminUtils.topicExists(zkClient, topicName)
    } finally {
      zkClient.close()
    }
  }
}
