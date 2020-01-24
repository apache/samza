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

import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.execution.StreamManager
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStreamPartition}

object KafkaUtil extends Logging {
  /**
   * Version number to track the format of the checkpoint log
   */
  val CHECKPOINT_LOG_VERSION_NUMBER = 1

  private def abs(n: Int) = if (n == Integer.MIN_VALUE) 0 else math.abs(n)

  /**
   * Partition key in the envelope must not be null.
   */
  def getIntegerPartitionKey(envelope: OutgoingMessageEnvelope, partitions: java.util.List[PartitionInfo]): Integer = {
    val numPartitions = partitions.size
    abs(envelope.getPartitionKey.hashCode()) % numPartitions
  }

  def getCheckpointTopic(jobName: String, jobId: String, config: Config) = {
    val checkpointTopic = "__samza_checkpoint_ver_%d_for_%s_%s" format(CHECKPOINT_LOG_VERSION_NUMBER,
      jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
    StreamManager.createUniqueNameForBatch(checkpointTopic, config)
  }

  /**
   * Converts the TopicPartition to SystemStreamPartition.
   *
   * @param topicPartition represents the TopicPartition.
   * @return an instance of the SystemStreamPartition.
   */
  def toSystemStreamPartition(systemName: String, topicPartition: TopicPartition): SystemStreamPartition = {
    val topic = topicPartition.topic()
    val partition = new Partition(topicPartition.partition)
    new SystemStreamPartition(systemName, topic, partition)
  }
}