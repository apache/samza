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

import com.google.common.annotations.VisibleForTesting
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.samza.config.ApplicationConfig.ApplicationMode
import org.apache.samza.config.KafkaConfig.Config2Kafka
import org.apache.samza.config.StorageConfig._
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.config._
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.{SystemAdmin, SystemConsumer, SystemFactory, SystemProducer}
import org.apache.samza.util._

object KafkaSystemFactory extends Logging {
  @VisibleForTesting
  def getInjectedProducerProperties(systemName: String, config: Config) = if (config.isChangelogSystem(systemName)) {
    warn("System name '%s' is being used as a changelog. Disabling compression since Kafka does not support compression for log compacted topics." format systemName)
    Map[String, String]("compression.type" -> "none")
  } else {
    Map[String, String]()
  }

  val CLIENTID_PRODUCER_PREFIX = "kafka-producer"
  val CLIENTID_CONSUMER_PREFIX = "kafka-consumer"
  val CLIENTID_ADMIN_PREFIX = "kafka-admin-consumer"
}

class KafkaSystemFactory extends SystemFactory with Logging {

  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry): SystemConsumer = {
    val metrics = new KafkaSystemConsumerMetrics(systemName, registry)

    val clientId = KafkaConsumerConfig.createClientId(KafkaSystemFactory.CLIENTID_CONSUMER_PREFIX, config);
    val kafkaConsumerConfig = KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, systemName, clientId);

    val kafkaConsumer = KafkaSystemConsumer.createKafkaConsumerImpl[Array[Byte], Array[Byte]](systemName, kafkaConsumerConfig)
    info("Created kafka consumer for system %s, clientId %s: %s" format (systemName, clientId, kafkaConsumer))

    val kafkaConsumerProxyFactory =
      new KafkaConsumerProxy.BaseFactory[Array[Byte], Array[Byte]](kafkaConsumer, systemName, clientId, metrics)

    val kafkaSystemConsumer = new KafkaSystemConsumer(kafkaConsumer, systemName, config, clientId,
      kafkaConsumerProxyFactory, metrics, new SystemClock)
    info("Created samza system consumer for system %s, config %s: %s" format(systemName, config, kafkaSystemConsumer))

    kafkaSystemConsumer
  }

  def getProducer(systemName: String, config: Config, registry: MetricsRegistry): SystemProducer = {
    val injectedProps = KafkaSystemFactory.getInjectedProducerProperties(systemName, config)
    val clientId = KafkaConsumerConfig.createClientId(KafkaSystemFactory.CLIENTID_PRODUCER_PREFIX, config);
    val producerConfig = config.getKafkaSystemProducerConfig(systemName, clientId, injectedProps)
    val getProducer = () => {
      new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    }
    val metrics = new KafkaSystemProducerMetrics(systemName, registry)

    // Unlike consumer, no need to use encoders here, since they come for free
    // inside the producer configs. Kafka's producer will handle all of this
    // for us.
    info("Creating kafka producer for system %s, producerClientId %s" format(systemName, clientId))

    new KafkaSystemProducer(
      systemName,
      new ExponentialSleepStrategy(initialDelayMs = producerConfig.reconnectIntervalMs),
      getProducer,
      metrics,
      dropProducerExceptions = config.getDropProducerErrors)
  }

  def getAdmin(systemName: String, config: Config): SystemAdmin = {
    // extract kafka client configs
    val clientId = KafkaConsumerConfig.createClientId(KafkaSystemFactory.CLIENTID_ADMIN_PREFIX, config);
    val consumerConfig = KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, systemName, clientId)

    new KafkaSystemAdmin(systemName, config, KafkaSystemConsumer.createKafkaConsumerImpl(systemName, consumerConfig))
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
        // only the override here
        val properties = new Properties()
        properties.putIfAbsent("retention.ms", String.valueOf(KafkaConfig.DEFAULT_RETENTION_MS_FOR_BATCH))
        (streamId, properties)
      }).toMap
    } else {
      Map()
    }
  }
}
