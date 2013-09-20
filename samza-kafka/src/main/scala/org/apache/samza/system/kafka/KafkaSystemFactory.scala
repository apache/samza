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

import org.apache.samza.util.KafkaUtil
import org.apache.samza.config.Config
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.config.KafkaConfig
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.config.StreamConfig.Config2Stream
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.util.ClientUtilTopicMetadataStore
import org.apache.samza.SamzaException
import scala.collection.JavaConversions._
import java.util.Properties
import kafka.producer.Producer
import kafka.producer.async.DefaultEventHandler
import kafka.utils.Utils
import org.apache.samza.util.Util
import kafka.serializer.Decoder
import kafka.serializer.DefaultDecoder
import org.apache.samza.system.SystemFactory

class KafkaSystemFactory extends SystemFactory {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = {
    val clientId = KafkaUtil.getClientId("samza-consumer", config)
    val metrics = new KafkaSystemConsumerMetrics(systemName, registry)

    // Kind of goofy to need a producer config for consumers, but we need metadata.
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId)
    val brokerListString = Option(producerConfig.brokerList)
      .getOrElse(throw new SamzaException("No broker list defined in config for %s." format systemName))
    val consumerConfig = config.getKafkaSystemConsumerConfig(systemName, clientId)

    // TODO could add stream-level overrides for timeout and buffer size
    val timeout = consumerConfig.socketTimeoutMs
    val bufferSize = consumerConfig.socketReceiveBufferBytes
    val autoOffsetResetDefault = consumerConfig.autoOffsetReset
    val autoOffsetResetTopics = config.getAutoOffsetResetTopics(systemName)
    val fetchThreshold = config.getConsumerFetchThreshold(systemName).getOrElse("0").toInt
    val offsetGetter = new GetOffset(autoOffsetResetDefault, autoOffsetResetTopics)
    val deserializer = config.getConsumerMsgDeserializerClass(systemName) match {
      case Some(deserializerClass) => Util.getObj[Decoder[Object]](deserializerClass)
      case _ => new DefaultDecoder().asInstanceOf[Decoder[Object]]
    }
    val keyDeserializer = config.getConsumerKeyDeserializerClass(systemName) match {
      case Some(deserializerClass) => Util.getObj[Decoder[Object]](deserializerClass)
      case _ => new DefaultDecoder().asInstanceOf[Decoder[Object]]
    }

    new KafkaSystemConsumer(
      systemName = systemName,
      brokerListString = brokerListString,
      metrics = metrics,
      clientId = clientId,
      timeout = timeout,
      bufferSize = bufferSize,
      fetchThreshold = fetchThreshold,
      offsetGetter = offsetGetter)
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = {
    val clientId = KafkaUtil.getClientId("samza-producer", config)
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId)
    val batchSize = Option(producerConfig.batchNumMessages)
      .getOrElse(1000)
    val reconnectIntervalMs = Option(producerConfig.retryBackoffMs)
      .getOrElse(10000)
    val getProducer = () => { new Producer[Object, Object](producerConfig) }
    val metrics = new KafkaSystemProducerMetrics(systemName, registry)

    // Unlike consumer, no need to use encoders here, since they come for free 
    // inside the producer configs. Kafka's producer will handle all of this 
    // for us.

    new KafkaSystemProducer(
      systemName,
      batchSize,
      reconnectIntervalMs,
      getProducer,
      metrics)
  }

  def getAdmin(systemName: String, config: Config) = {
    val clientId = KafkaUtil.getClientId("samza-admin", config)
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId)
    val brokerListString = Option(producerConfig.brokerList)
      .getOrElse(throw new SamzaException("No broker list defined in config for %s." format systemName))

    new KafkaSystemAdmin(systemName, brokerListString, clientId)
  }
}
