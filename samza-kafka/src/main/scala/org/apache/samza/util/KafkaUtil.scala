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

import java.util.concurrent.atomic.AtomicLong

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.common.PartitionInfo
import org.apache.samza.config.{Config, ConfigException}
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.execution.StreamManager
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.kafka.common.errors.ReplicaNotAvailableException
import org.apache.kafka.common.protocol.Errors

object KafkaUtil extends Logging {
  /**
   * Version number to track the format of the checkpoint log
   */
  val CHECKPOINT_LOG_VERSION_NUMBER = 1
  val counter = new AtomicLong(0)

  private def abs(n: Int) = if (n == Integer.MIN_VALUE) 0 else math.abs(n)

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
   * Exactly the same as Kafka's ErrorMapping.maybeThrowException
   * implementation, except suppresses ReplicaNotAvailableException exceptions.
   * According to the Kafka
   * <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol">protocol
   * docs</a>, ReplicaNotAvailableException can be safely ignored.
   */
  def maybeThrowException(e: Exception) {
    try {
      if (e != null)
        throw e
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
    code != Errors.NONE.code() && code != Errors.REPLICA_NOT_AVAILABLE.code()
  }
}

class KafkaUtil(val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                val connectZk: () => ZkUtils) extends Logging {

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
