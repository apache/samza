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

package org.apache.samza.migration

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.kafka.{KafkaCheckpointManager, KafkaCheckpointManagerFactory}
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.coordinator.stream.messages.{CoordinatorStreamMessage, SetMigrationMetaMessage}
import org.apache.samza.coordinator.stream.{CoordinatorStreamSystemConsumer, CoordinatorStreamSystemFactory, CoordinatorStreamSystemProducer}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.storage.ChangelogPartitionManager
import org.apache.samza.util._

/**
 * Migrates changelog partition mapping from checkpoint topic to coordinator stream
 */
class KafkaCheckpointMigration extends MigrationPlan with Logging {
  val source = "CHECKPOINTMIGRATION"
  val migrationKey = "CheckpointMigration09to10"
  val migrationVal = "true"

  var connectZk: () => ZkClient = null

  private def getCheckpointSystemName(config: Config): String = {
    config
      .getCheckpointSystem
      .getOrElse(throw new SamzaException("no system defined for Kafka's checkpoint manager."))
  }

  private def getClientId(config: Config): String = {
    KafkaUtil.getClientId("samza-checkpoint-manager", config)
  }

  private def getTopicMetadataStore(config: Config): TopicMetadataStore = {
    val clientId = getClientId(config)
    val systemName = getCheckpointSystemName(config)

    val producerConfig = config.getKafkaSystemProducerConfig(
      systemName,
      clientId,
      KafkaCheckpointManagerFactory.INJECTED_PRODUCER_PROPERTIES)

    val consumerConfig = config.getKafkaSystemConsumerConfig(
      systemName,
      clientId)

    new ClientUtilTopicMetadataStore(producerConfig.bootsrapServers, clientId, consumerConfig.socketTimeoutMs)
  }

  private def getConnectZk(config: Config): () => ZkClient = {
    val clientId = getClientId(config)

    val checkpointSystemName = getCheckpointSystemName(config)

    val consumerConfig = config.getKafkaSystemConsumerConfig(
      checkpointSystemName,
      clientId)

    val zkConnectString = Option(consumerConfig.zkConnect)
      .getOrElse(throw new SamzaException("no zookeeper.connect defined in config"))
    () => {
      new ZkClient(zkConnectString, 6000, 6000, ZKStringSerializer)
    }
  }

  override def migrate(config: Config) {
    val jobName = config.getName.getOrElse(throw new SamzaException("Cannot find job name. Cannot proceed with migration."))
    val jobId = config.getJobId.getOrElse("1")

    val checkpointTopicName = KafkaUtil.getCheckpointTopic(jobName, jobId)

    val coordinatorSystemFactory = new CoordinatorStreamSystemFactory
    val coordinatorSystemConsumer = coordinatorSystemFactory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
    val coordinatorSystemProducer = coordinatorSystemFactory.getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap)

    val checkpointManager = new KafkaCheckpointManagerFactory().getCheckpointManager(config, new NoOpMetricsRegistry).asInstanceOf[KafkaCheckpointManager]

    val kafkaUtil = new KafkaUtil(new ExponentialSleepStrategy, getConnectZk(config))

    // make sure to validate that we only perform migration when checkpoint topic exists
    if (kafkaUtil.topicExists(checkpointTopicName)) {
      kafkaUtil.validateTopicPartitionCount(
        checkpointTopicName,
        getCheckpointSystemName(config),
        getTopicMetadataStore(config),
        1)

      if (migrationVerification(coordinatorSystemConsumer)) {
        info("Migration %s was already performed, doing nothing" format migrationKey)
        return
      }

      info("No previous migration for %s were detected, performing migration" format migrationKey)

      info("Loading changelog partition mapping from checkpoint topic - %s" format checkpointTopicName)
      val changelogMap = checkpointManager.readChangeLogPartitionMapping()
      checkpointManager.stop

      info("Writing changelog to coordinator stream topic - %s" format Util.getCoordinatorStreamName(jobName, jobId))
      val changelogPartitionManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, source)
      changelogPartitionManager.start()
      changelogPartitionManager.writeChangeLogPartitionMapping(changelogMap)
      changelogPartitionManager.stop()
    }
    migrationCompletionMark(coordinatorSystemProducer)

  }

  def migrationVerification(coordinatorSystemConsumer : CoordinatorStreamSystemConsumer): Boolean = {
    coordinatorSystemConsumer.register()
    coordinatorSystemConsumer.start()
    coordinatorSystemConsumer.bootstrap()
    val stream = coordinatorSystemConsumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE)
    val message = new SetMigrationMetaMessage(source, migrationKey, migrationVal)
    stream.contains(message.asInstanceOf[CoordinatorStreamMessage])
  }

  def migrationCompletionMark(coordinatorSystemProducer: CoordinatorStreamSystemProducer) = {
    info("Marking completion of migration %s" format migrationKey)
    val message = new SetMigrationMetaMessage(source, migrationKey, migrationVal)
    coordinatorSystemProducer.start()
    coordinatorSystemProducer.send(message)
    coordinatorSystemProducer.stop()
  }

}
