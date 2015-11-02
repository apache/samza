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

package org.apache.samza.checkpoint.kafka

import java.util.Properties

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.{CheckpointManager, CheckpointManagerFactory}
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.config.{Config, KafkaConfig}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.util.{ClientUtilTopicMetadataStore, KafkaUtil, Logging}

object KafkaCheckpointManagerFactory {
  val INJECTED_PRODUCER_PROPERTIES = Map(
    "acks" -> "all",
    // Forcibly disable compression because Kafka doesn't support compression
    // on log compacted topics. Details in SAMZA-586.
    "compression.type" -> "none")

  // Set the checkpoint topic configs to have a very small segment size and
  // enable log compaction. This keeps job startup time small since there 
  // are fewer useless (overwritten) messages to read from the checkpoint 
  // topic.
  def getCheckpointTopicProperties(config: Config) = {
    val segmentBytes: Int = if (config == null) {
      KafkaConfig.DEFAULT_CHECKPOINT_SEGMENT_BYTES
    } else {
      config.getCheckpointSegmentBytes()
    }
    (new Properties /: Map(
      "cleanup.policy" -> "compact",
      "segment.bytes" -> String.valueOf(segmentBytes))) { case (props, (k, v)) => props.put(k, v); props }
  }
}

class KafkaCheckpointManagerFactory extends CheckpointManagerFactory with Logging {
  import org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory._

  def getCheckpointManager(config: Config, registry: MetricsRegistry): CheckpointManager = {
    val clientId = KafkaUtil.getClientId("samza-checkpoint-manager", config)
    val jobName = config.getName.getOrElse(throw new SamzaException("Missing job name in configs"))
    val jobId = config.getJobId.getOrElse("1")

    val systemName = config
      .getCheckpointSystem
      .getOrElse(throw new SamzaException("no system defined for Kafka's checkpoint manager."))

    val producerConfig = config.getKafkaSystemProducerConfig(
      systemName,
      clientId,
      INJECTED_PRODUCER_PROPERTIES)
    val connectProducer = () => {
      new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    }

    val consumerConfig = config.getKafkaSystemConsumerConfig(systemName, clientId)
    val zkConnect = Option(consumerConfig.zkConnect)
      .getOrElse(throw new SamzaException("no zookeeper.connect defined in config"))
    val connectZk = () => {
      new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer)
    }
    val socketTimeout = consumerConfig.socketTimeoutMs


    new KafkaCheckpointManager(
      clientId,
      KafkaUtil.getCheckpointTopic(jobName, jobId),
      systemName,
      config.getCheckpointReplicationFactor.getOrElse("3").toInt,
      socketTimeout,
      consumerConfig.socketReceiveBufferBytes,
      consumerConfig.fetchMessageMaxBytes,            // must be > buffer size
      new ClientUtilTopicMetadataStore(producerConfig.bootsrapServers, clientId, socketTimeout),
      connectProducer,
      connectZk,
      config.getSystemStreamPartitionGrouperFactory,      // To find out the SSPGrouperFactory class so it can be included/verified in the key
      checkpointTopicProperties = getCheckpointTopicProperties(config))
  }
}