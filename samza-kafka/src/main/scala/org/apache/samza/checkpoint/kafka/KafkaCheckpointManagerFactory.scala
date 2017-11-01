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

import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.{CheckpointManager, CheckpointManagerFactory}
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config._
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.kafka.KafkaStreamSpec
import org.apache.samza.util.{KafkaUtil, Logging, Util, _}

class KafkaCheckpointManagerFactory extends CheckpointManagerFactory with Logging {

  def getCheckpointManager(config: Config, registry: MetricsRegistry): CheckpointManager = {
    val jobName = config.getName.getOrElse(throw new SamzaException("Missing job name in configs"))
    val jobId = config.getJobId.getOrElse("1")

    val kafkaConfig = new KafkaConfig(config)
    val checkpointSystemName = kafkaConfig.getCheckpointSystem.getOrElse(
      throw new SamzaException("No system defined for Kafka's checkpoint manager."))
    val checkpointSystemFactoryName = new SystemConfig(config)
      .getSystemFactory(checkpointSystemName)
      .getOrElse(throw new SamzaException("Missing configuration: " + SystemConfig.SYSTEM_FACTORY format checkpointSystemName))

    val checkpointSystemFactory = Util.getObj[SystemFactory](checkpointSystemFactoryName)
    val numCheckpointPartitions = 1

    val checkpointSpec : KafkaStreamSpec = new KafkaStreamSpec("samza-unused-checkpoint-stream-id",
        KafkaUtil.getCheckpointTopic(jobName, jobId, config), checkpointSystemName, numCheckpointPartitions,
        kafkaConfig.getCheckpointReplicationFactor.get.toInt,
        kafkaConfig.getCheckpointTopicProperties())

    new KafkaCheckpointManager(checkpointSpec, checkpointSystemFactory, config.failOnCheckpointValidation, config,
      new NoOpMetricsRegistry)
  }
}
