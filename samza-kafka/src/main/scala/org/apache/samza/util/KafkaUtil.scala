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
import org.apache.kafka.common.PartitionInfo
import org.apache.samza.config.Config
import org.apache.samza.config.ConfigException
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.system.OutgoingMessageEnvelope
import kafka.common.ErrorMapping
import kafka.common.ReplicaNotAvailableException

object KafkaUtil extends Logging {
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
