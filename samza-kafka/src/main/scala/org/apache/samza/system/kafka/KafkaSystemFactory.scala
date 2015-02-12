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

import org.apache.samza.util.{Logging, KafkaUtil, ExponentialSleepStrategy, ClientUtilTopicMetadataStore}
import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.samza.system.SystemFactory
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer


class KafkaSystemFactory extends SystemFactory with Logging {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    val clientId = KafkaUtil.getClientId("samza-consumer", config)
    val metrics = new KafkaSystemConsumerMetrics(systemName, registry)

    // Kind of goofy to need a producer config for consumers, but we need metadata.
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId)
    val bootstrapServers = producerConfig.bootsrapServers
    val consumerConfig = config.getKafkaSystemConsumerConfig(systemName, clientId)

    val timeout = consumerConfig.socketTimeoutMs
    val bufferSize = consumerConfig.socketReceiveBufferBytes
    val fetchSize = new StreamFetchSizes(consumerConfig.fetchMessageMaxBytes, config.getFetchMessageMaxBytesTopics(systemName))
    val consumerMinSize = consumerConfig.fetchMinBytes
    val consumerMaxWait = consumerConfig.fetchWaitMaxMs
    val autoOffsetResetDefault = consumerConfig.autoOffsetReset
    val autoOffsetResetTopics = config.getAutoOffsetResetTopics(systemName)
    val fetchThreshold = config.getConsumerFetchThreshold(systemName).getOrElse("50000").toInt
    val offsetGetter = new GetOffset(autoOffsetResetDefault, autoOffsetResetTopics)
    val metadataStore = new ClientUtilTopicMetadataStore(bootstrapServers, clientId, timeout)

    new KafkaSystemConsumer(
      systemName = systemName,
      metrics = metrics,
      metadataStore = metadataStore,
      clientId = clientId,
      timeout = timeout,
      bufferSize = bufferSize,
      fetchSize = fetchSize,
      consumerMinSize = consumerMinSize,
      consumerMaxWait = consumerMaxWait,
      fetchThreshold = fetchThreshold,
      offsetGetter = offsetGetter)
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    val clientId = KafkaUtil.getClientId("samza-producer", config)
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId)
    val getProducer = () => { new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties) }
    val metrics = new KafkaSystemProducerMetrics(systemName, registry)

    // Unlike consumer, no need to use encoders here, since they come for free 
    // inside the producer configs. Kafka's producer will handle all of this 
    // for us.

    new KafkaSystemProducer(
      systemName,
      new ExponentialSleepStrategy(initialDelayMs = producerConfig.reconnectIntervalMs),
      getProducer,
      metrics)
  }

  def getAdmin(systemName: String, config: Config) = {
    val clientId = KafkaUtil.getClientId("samza-admin", config)
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId)
    val bootstrapServers = producerConfig.bootsrapServers
    val consumerConfig = config.getKafkaSystemConsumerConfig(systemName, clientId)
    val timeout = consumerConfig.socketTimeoutMs
    val bufferSize = consumerConfig.socketReceiveBufferBytes
    val storeToChangelog = config.getKafkaChangelogEnabledStores()

    // Construct the meta information for each topic, if the replication factor is not defined, we use 2 as the number of replicas for the change log stream.
    val topicMetaInformation = storeToChangelog.map{case (storeName, topicName) =>
    {
       val replicationFactor = config.getChangelogStreamReplicationFactor(storeName).getOrElse("2").toInt
       val changelogInfo = ChangelogInfo(replicationFactor, config.getChangelogKafkaProperties(storeName))
       info("Creating topic meta information for topic: %s with replication factor: %s" format (topicName, replicationFactor))
       (topicName, changelogInfo)
    }}.toMap

    new KafkaSystemAdmin(
      systemName,
      bootstrapServers,
      timeout,
      bufferSize,
      clientId,
      () => new ZkClient(consumerConfig.zkConnect, 6000, 6000, ZKStringSerializer),
      topicMetaInformation)
  }
}
