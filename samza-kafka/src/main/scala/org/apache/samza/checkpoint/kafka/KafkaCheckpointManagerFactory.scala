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

import com.google.common.collect.ImmutableMap
import kafka.utils.ZkUtils
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.{CheckpointManager, CheckpointManagerFactory}
import org.apache.samza.config.ApplicationConfig.ApplicationMode
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config._
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.kafka.KafkaStreamSpec
import org.apache.samza.util.{ClientUtilTopicMetadataStore, KafkaUtil, Logging, Util, _}


object KafkaCheckpointManagerFactory {
  val INJECTED_PRODUCER_PROPERTIES = Map(
    "acks" -> "all",
    // Forcibly disable compression because Kafka doesn't support compression
    // on log compacted topics. Details in SAMZA-586.
    "compression.type" -> "none")


  def getCheckpointTopicProperties(config: Config) = {
    val segmentBytes: Int = if (config == null) {
      KafkaConfig.DEFAULT_CHECKPOINT_SEGMENT_BYTES
    } else {
      new KafkaConfig(config).getCheckpointSegmentBytes()
    }
    val props = new Properties()
    props.putAll(ImmutableMap.of("cleanup.policy", "compact",
        "segment.bytes", String.valueOf(segmentBytes)))
    props
  }

  /**
   * Get the checkpoint system and system factory from the configuration
   * @param config
   * @return system name and system factory
   */
  def getCheckpointSystemStreamAndFactory(config: Config) = {

    val kafkaConfig = new KafkaConfig(config)
    val systemName = kafkaConfig.getCheckpointSystem.getOrElse(throw new SamzaException("no system defined for Kafka's checkpoint manager."))

    val systemFactoryClassName = new SystemConfig(config)
            .getSystemFactory(systemName)
            .getOrElse(throw new SamzaException("Missing configuration: " + SystemConfig.SYSTEM_FACTORY format systemName))
    val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
    (systemName, systemFactory)
  }
}

class KafkaCheckpointManagerFactory extends CheckpointManagerFactory with Logging {
  import org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory._

  def getCheckpointManager(config: Config, registry: MetricsRegistry): CheckpointManager = {
    val clientId = KafkaUtil.getClientId("samza-checkpoint-manager", config)
    val jobName = config.getName.getOrElse(throw new SamzaException("Missing job name in configs"))
    val jobId = config.getJobId.getOrElse("1")

    val (systemName: String, systemFactory : SystemFactory) =  getCheckpointSystemStreamAndFactory(config)

    val kafkaConfig = new KafkaConfig(config)

    val noOpMetricsRegistry = new NoOpMetricsRegistry()

    val consumerConfig = kafkaConfig.getKafkaSystemConsumerConfig(systemName, clientId)
    val zkConnect = Option(consumerConfig.zkConnect)
      .getOrElse(throw new SamzaException("no zookeeper.connect defined in config"))
    val connectZk = () => {
      ZkUtils(zkConnect, 6000, 6000, false)
    }

    val checkpointSpec : KafkaStreamSpec = new KafkaStreamSpec("samza-unused-checkpoint-stream-id",
        KafkaUtil.getCheckpointTopic(jobName, jobId, config), systemName, 1,
        kafkaConfig.getCheckpointReplicationFactor.get.toInt,
        getCheckpointTopicProperties(config));

    new KafkaCheckpointManager(checkpointSpec, systemFactory, config.failOnCheckpointValidation, config, noOpMetricsRegistry)
  }
}
