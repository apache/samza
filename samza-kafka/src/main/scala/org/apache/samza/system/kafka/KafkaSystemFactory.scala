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

import java.util.Properties

import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.samza.SamzaException
import org.apache.samza.config.ApplicationConfig.ApplicationMode
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.config.StorageConfig._
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config._
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.{SystemAdmin, SystemConsumer, SystemFactory, SystemProducer}
import org.apache.samza.util._

object KafkaSystemFactory extends Logging {
  def getInjectedProducerProperties(systemName: String, config: Config) = if (config.isChangelogSystem(systemName)) {
    warn("System name '%s' is being used as a changelog. Disabling compression since Kafka does not support compression for log compacted topics." format systemName)
    Map[String, String]("compression.type" -> "none")
  } else {
    Map[String, String]()
  }
}

class KafkaSystemFactory extends SystemFactory with Logging {

  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry): SystemConsumer = {
    val clientId = KafkaConsumerConfig.getConsumerClientId( config)
    val metrics = new KafkaSystemConsumerMetrics(systemName, registry)

    val kafkaConsumer = KafkaSystemConsumer.getKafkaConsumerImpl(systemName, clientId, config)
    info("Created kafka consumer for system %s, clientId %s: %s" format (systemName, clientId, kafkaConsumer))

    val kafkaSystemConsumer = new KafkaSystemConsumer(kafkaConsumer, systemName, config, clientId, metrics, new SystemClock)
    info("Created samza system consumer %s" format  (kafkaSystemConsumer.toString))

    kafkaSystemConsumer
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry): SystemProducer = {
    val clientId = KafkaConsumerConfig.getProducerClientId(config)
    val injectedProps = KafkaSystemFactory.getInjectedProducerProperties(systemName, config)
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId, injectedProps)
    val getProducer = () => {
      new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    }
    val metrics = new KafkaSystemProducerMetrics(systemName, registry)

    // Unlike consumer, no need to use encoders here, since they come for free
    // inside the producer configs. Kafka's producer will handle all of this
    // for us.

    new KafkaSystemProducer(
      systemName,
      new ExponentialSleepStrategy(initialDelayMs = producerConfig.reconnectIntervalMs),
      getProducer,
      metrics,
      dropProducerExceptions = config.getDropProducerError)
  }

  def getAdmin(systemName: String, config: Config): SystemAdmin = {
    val clientId = KafkaConsumerConfig.getAdminClientId(config)
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId)
    val bootstrapServers = producerConfig.bootsrapServers
    val consumerConfig = config.getKafkaSystemConsumerConfig(systemName, clientId)
    val timeout = consumerConfig.socketTimeoutMs
    val bufferSize = consumerConfig.socketReceiveBufferBytes
    val zkConnect = Option(consumerConfig.zkConnect)
      .getOrElse(throw new SamzaException("no zookeeper.connect defined in config"))
    val connectZk = () => {
      ZkUtils(zkConnect, 6000, 6000, false)
    }
    val coordinatorStreamProperties = getCoordinatorTopicProperties(config)
    val coordinatorStreamReplicationFactor = config.getCoordinatorReplicationFactor.toInt
    val storeToChangelog = config.getKafkaChangelogEnabledStores()
    // Construct the meta information for each topic, if the replication factor is not defined, we use 2 as the number of replicas for the change log stream.
    val topicMetaInformation = storeToChangelog.map { case (storeName, topicName) => {
      val replicationFactor = config.getChangelogStreamReplicationFactor(storeName).toInt
      val changelogInfo = ChangelogInfo(replicationFactor, config.getChangelogKafkaProperties(storeName))
      info("Creating topic meta information for topic: %s with replication factor: %s" format(topicName, replicationFactor))
      (topicName, changelogInfo)
    }
    }

    val deleteCommittedMessages = config.deleteCommittedMessages(systemName).exists(isEnabled => isEnabled.toBoolean)
    val intermediateStreamProperties: Map[String, Properties] = getIntermediateStreamProperties(config)
    new KafkaSystemAdmin(
      systemName,
      bootstrapServers,
      connectZk,
      coordinatorStreamProperties,
      coordinatorStreamReplicationFactor,
      timeout,
      bufferSize,
      clientId,
      topicMetaInformation,
      intermediateStreamProperties,
      deleteCommittedMessages)
  }

  def getCoordinatorTopicProperties(config: Config) = {
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
}
