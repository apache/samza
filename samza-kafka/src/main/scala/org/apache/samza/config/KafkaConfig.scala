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

package org.apache.samza.config


import java.util.regex.Pattern

import org.apache.samza.util.Util
import org.apache.samza.util.Logging
import scala.collection.JavaConversions._
import kafka.consumer.ConsumerConfig
import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.samza.SamzaException
import java.util
import scala.collection.JavaConverters._
import org.apache.samza.system.kafka.KafkaSystemFactory
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.kafka.common.serialization.ByteArraySerializer

object KafkaConfig {
  val REGEX_RESOLVED_STREAMS = "job.config.rewriter.%s.regex"
  val REGEX_RESOLVED_SYSTEM = "job.config.rewriter.%s.system"
  val REGEX_INHERITED_CONFIG = "job.config.rewriter.%s.config"

  val CHECKPOINT_SYSTEM = "task.checkpoint.system"
  val CHECKPOINT_REPLICATION_FACTOR = "task.checkpoint.replication.factor"
  val CHECKPOINT_SEGMENT_BYTES = "task.checkpoint.segment.bytes"

  val CHANGELOG_STREAM_REPLICATION_FACTOR = "stores.%s.changelog.replication.factor"
  val CHANGELOG_STREAM_KAFKA_SETTINGS = "stores.%s.changelog.kafka."
  // The default segment size to use for changelog topics
  val CHANGELOG_DEFAULT_SEGMENT_SIZE = "536870912"

  // Helper regular expression definitions to extract/match configurations
  val CHANGELOG_STREAM_NAMES_REGEX = "stores\\.(.*)\\.changelog$"

  /**
   * Defines how low a queue can get for a single system/stream/partition
   * combination before trying to fetch more messages for it.
   */
  val CONSUMER_FETCH_THRESHOLD = SystemConfig.SYSTEM_PREFIX + "samza.fetch.threshold"

  val DEFAULT_CHECKPOINT_SEGMENT_BYTES = 26214400

  /**
   * Defines how many bytes to use for the buffered prefetch messages for job as a whole.
   * The bytes for a single system/stream/partition are computed based on this.
   * This fetches wholes messages, hence this bytes limit is a soft one, and the actual usage can be
   * the bytes limit + size of max message in the partition for a given stream.
   * If the value of this property is > 0 then this takes precedence over CONSUMER_FETCH_THRESHOLD config.
   */
  val CONSUMER_FETCH_THRESHOLD_BYTES = SystemConfig.SYSTEM_PREFIX + "samza.fetch.threshold.bytes"

  implicit def Config2Kafka(config: Config) = new KafkaConfig(config)
}

class KafkaConfig(config: Config) extends ScalaMapConfig(config) {
  // checkpoints
  def getCheckpointSystem = getOption(KafkaConfig.CHECKPOINT_SYSTEM)
  def getCheckpointReplicationFactor() = getOption(KafkaConfig.CHECKPOINT_REPLICATION_FACTOR)
  def getCheckpointSegmentBytes() = getInt(KafkaConfig.CHECKPOINT_SEGMENT_BYTES, KafkaConfig.DEFAULT_CHECKPOINT_SEGMENT_BYTES)
  // custom consumer config
  def getConsumerFetchThreshold(name: String) = getOption(KafkaConfig.CONSUMER_FETCH_THRESHOLD format name)
  def getConsumerFetchThresholdBytes(name: String) = getOption(KafkaConfig.CONSUMER_FETCH_THRESHOLD_BYTES format name)
  def isConsumerFetchThresholdBytesEnabled(name: String): Boolean = getConsumerFetchThresholdBytes(name).getOrElse("-1").toLong > 0


  /**
   * Returns a map of topic -> fetch.message.max.bytes value for all streams that
   * are defined with this property in the config.
   */
  def getFetchMessageMaxBytesTopics(systemName: String) = {
    val subConf = config.subset("systems.%s.streams." format systemName, true)
    subConf
      .filterKeys(k => k.endsWith(".consumer.fetch.message.max.bytes"))
      .map {
        case (fetchMessageMaxBytes, fetchSizeValue) =>
          (fetchMessageMaxBytes.replace(".consumer.fetch.message.max.bytes", ""), fetchSizeValue.toInt)
      }.toMap
  }

  /**
   * Returns a map of topic -> auto.offset.reset value for all streams that
   * are defined with this property in the config.
   */
  def getAutoOffsetResetTopics(systemName: String) = {
    val subConf = config.subset("systems.%s.streams." format systemName, true)
    subConf
      .filterKeys(k => k.endsWith(".consumer.auto.offset.reset"))
      .map {
        case (topicAutoOffsetReset, resetValue) =>
          (topicAutoOffsetReset.replace(".consumer.auto.offset.reset", ""), resetValue)
      }.toMap
  }

  // regex resolver
  def getRegexResolvedStreams(rewriterName: String) = getOption(KafkaConfig.REGEX_RESOLVED_STREAMS format rewriterName)
  def getRegexResolvedSystem(rewriterName: String) = getOption(KafkaConfig.REGEX_RESOLVED_SYSTEM format rewriterName)
  def getRegexResolvedInheritedConfig(rewriterName: String) = config.subset((KafkaConfig.REGEX_INHERITED_CONFIG format rewriterName) + ".", true)
  def getChangelogStreamReplicationFactor(name: String) = getOption(KafkaConfig.CHANGELOG_STREAM_REPLICATION_FACTOR format name)

  // The method returns a map of storenames to changelog topic names, which are configured to use kafka as the changelog stream
  def getKafkaChangelogEnabledStores() = {
    val changelogConfigs = config.regexSubset(KafkaConfig.CHANGELOG_STREAM_NAMES_REGEX).asScala
    var storeToChangelog = Map[String, String]()
    for((changelogConfig, changelogName) <- changelogConfigs){
      // Lookup the factory for this particular stream and verify if it's a kafka system
      val systemStream = Util.getSystemStreamFromNames(changelogName)
      val factoryName = config.getSystemFactory(systemStream.getSystem).getOrElse(new SamzaException("Unable to determine factory for system: " + systemStream.getSystem))
      if(classOf[KafkaSystemFactory].getCanonicalName == factoryName){
        val pattern = Pattern.compile(KafkaConfig.CHANGELOG_STREAM_NAMES_REGEX)
        val matcher = pattern.matcher(changelogConfig)
        val storeName = if(matcher.find()) matcher.group(1) else throw new SamzaException("Unable to find store name in the changelog configuration: " + changelogConfig + " with SystemStream: " + systemStream)
        storeToChangelog += storeName -> systemStream.getStream
      }
    }
    storeToChangelog
  }

  // Get all kafka properties for changelog stream topic creation
  def getChangelogKafkaProperties(name: String) = {
    val filteredConfigs = config.subset(KafkaConfig.CHANGELOG_STREAM_KAFKA_SETTINGS format name, true)
    val kafkaChangeLogProperties = new Properties
    kafkaChangeLogProperties.setProperty("cleanup.policy", "compact")
    kafkaChangeLogProperties.setProperty("segment.bytes", KafkaConfig.CHANGELOG_DEFAULT_SEGMENT_SIZE)
    filteredConfigs.foreach{kv => kafkaChangeLogProperties.setProperty(kv._1, kv._2)}
    kafkaChangeLogProperties
  }

  // kafka config
  def getKafkaSystemConsumerConfig(
    systemName: String,
    clientId: String = "undefined-samza-consumer-%s" format UUID.randomUUID.toString,
    groupId: String = "undefined-samza-consumer-group-%s" format UUID.randomUUID.toString,
    injectedProps: Map[String, String] = Map()) = {

    val subConf = config.subset("systems.%s.consumer." format systemName, true)
    val consumerProps = new Properties()
    consumerProps.putAll(subConf)
    consumerProps.put("group.id", groupId)
    consumerProps.put("client.id", clientId)
    consumerProps.putAll(injectedProps)
    new ConsumerConfig(consumerProps)
  }

  def getKafkaSystemProducerConfig(
    systemName: String,
    clientId: String = "undefined-samza-producer-%s" format UUID.randomUUID.toString,
    injectedProps: Map[String, String] = Map()) = {

    val subConf = config.subset("systems.%s.producer." format systemName, true)
    val producerProps = new util.HashMap[String, Object]()
    producerProps.putAll(subConf)
    producerProps.put("client.id", clientId)
    producerProps.putAll(injectedProps)
    new KafkaProducerConfig(systemName, clientId, producerProps)
  }
}

class KafkaProducerConfig(val systemName: String,
                          val clientId: String = "",
                          properties: java.util.Map[String, Object] = new util.HashMap[String, Object]()) extends Logging {

  // Copied from new Kafka API - Workaround until KAFKA-1794 is resolved
  val RECONNECT_BACKOFF_MS_DEFAULT = 10L

  //Overrides specific to samza-kafka (these are considered as defaults in Samza & can be overridden by user
  val MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT: java.lang.Integer = 1.asInstanceOf[Integer]
  val RETRIES_DEFAULT: java.lang.Integer = Integer.MAX_VALUE

  def getProducerProperties = {
    val byteArraySerializerClassName = classOf[ByteArraySerializer].getCanonicalName
    val producerProperties: java.util.Map[String, Object] = new util.HashMap[String, Object]()
    producerProperties.putAll(properties)

    if(!producerProperties.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
      debug("%s undefined. Defaulting to %s." format (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, byteArraySerializerClassName))
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, byteArraySerializerClassName)
    }

    if(!producerProperties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
      debug("%s undefined. Defaulting to %s." format (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, byteArraySerializerClassName))
      producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, byteArraySerializerClassName)
    }

    if(producerProperties.containsKey(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
        && producerProperties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).asInstanceOf[String].toInt > MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT) {
      warn("Setting '%s' to a value other than %d does not guarantee message ordering because new messages will be sent without waiting for previous ones to be acknowledged." 
          format (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT))
    } else {
      producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT)
    }

    if(producerProperties.containsKey(ProducerConfig.RETRIES_CONFIG) 
        && producerProperties.get(ProducerConfig.RETRIES_CONFIG).asInstanceOf[String].toInt < RETRIES_DEFAULT) {
        warn("Samza does not provide producer failure handling. Consider setting '%s' to a large value, like Int.MAX." format ProducerConfig.RETRIES_CONFIG)
    } else {
      // Retries config is set to Max so that when all attempts fail, Samza also fails the send. We do not have any special handler
      // for producer failure
      producerProperties.put(ProducerConfig.RETRIES_CONFIG, RETRIES_DEFAULT)
    }
    
    producerProperties
  }

  val reconnectIntervalMs =  Option(properties.get(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG))
          .getOrElse(RECONNECT_BACKOFF_MS_DEFAULT).asInstanceOf[Long]

  val bootsrapServers = {
    if(properties.containsKey("metadata.broker.list"))
      warn("Kafka producer configuration contains 'metadata.broker.list'. This configuration is deprecated . Samza has been upgraded " +
             "to use Kafka's new producer API. Please update your configurations based on the documentation at http://kafka.apache.org/documentation.html#newproducerconfigs")
    Option(properties.get("bootstrap.servers"))
            .getOrElse(throw new SamzaException("No bootstrap servers defined in config for %s." format systemName))
            .asInstanceOf[String]
  }
}
