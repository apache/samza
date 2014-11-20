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


import org.apache.samza.SamzaException

import scala.collection.JavaConversions._
import kafka.consumer.ConsumerConfig
import java.util.Properties
import kafka.producer.ProducerConfig
import java.util.UUID
import scala.collection.JavaConverters._
import org.apache.samza.system.kafka.KafkaSystemFactory
import scala.collection.mutable.ArrayBuffer


object KafkaConfig {
  val REGEX_RESOLVED_STREAMS = "job.config.rewriter.%s.regex"
  val REGEX_RESOLVED_SYSTEM = "job.config.rewriter.%s.system"
  val REGEX_INHERITED_CONFIG = "job.config.rewriter.%s.config"

  val CHECKPOINT_SYSTEM = "task.checkpoint.system"
  val CHECKPOINT_REPLICATION_FACTOR = "task.checkpoint.replication.factor"
  val CHECKPOINT_SEGMENT_BYTES = "task.checkpoint.segment.bytes"

  val CHANGELOG_STREAM_REPLICATION_FACTOR = "stores.%s.changelog.replication.factor"
  val CHANGELOG_STREAM_KAFKA_SETTINGS  = "stores.%s.changelog.kafka"
  val CHANGELOG_STREAM_NAMES_REGEX = "stores\\..*\\.changelog$"

  /**
   * Defines how low a queue can get for a single system/stream/partition
   * combination before trying to fetch more messages for it.
   */
  val CONSUMER_FETCH_THRESHOLD = SystemConfig.SYSTEM_PREFIX + "samza.fetch.threshold"

  implicit def Config2Kafka(config: Config) = new KafkaConfig(config)
}

class KafkaConfig(config: Config) extends ScalaMapConfig(config) {
  // checkpoints
  def getCheckpointSystem = getOption(KafkaConfig.CHECKPOINT_SYSTEM)
  def getCheckpointReplicationFactor() = getOption(KafkaConfig.CHECKPOINT_REPLICATION_FACTOR)
  def getCheckpointSegmentBytes() = getOption(KafkaConfig.CHECKPOINT_SEGMENT_BYTES)

  // custom consumer config
  def getConsumerFetchThreshold(name: String) = getOption(KafkaConfig.CONSUMER_FETCH_THRESHOLD format name)

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
  def getChangelogTopicNames() = {
    val changelogConfigs = config.regexSubset(KafkaConfig.CHANGELOG_STREAM_NAMES_REGEX).asScala
    val topicNamesList = ArrayBuffer[String]()
    for((changelogConfig, changelogName) <- changelogConfigs){
      // Lookup the factory for this particular stream and verify if it's a kafka system
      val changelogNameSplit = changelogName.split("\\.")
      if(changelogNameSplit.length < 2) throw new SamzaException("Changelog name not in expected format")
      val factoryName = config.get(String.format(SystemConfig.SYSTEM_FACTORY, changelogNameSplit(0)))
      if(classOf[KafkaSystemFactory].getCanonicalName == factoryName){
        topicNamesList += changelogNameSplit(1)
      }
    }
    topicNamesList.toList
  }

  // Get all kafka properties for changelog stream topic creation
  def getChangelogKafkaProperties(name: String) = {
    val filteredConfigs = config.subset(KafkaConfig.CHANGELOG_STREAM_KAFKA_SETTINGS format name, true)

    val kafkaChangeLogProperties = new Properties
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
    val producerProps = new Properties()
    producerProps.putAll(subConf)
    producerProps.put("client.id", clientId)
    producerProps.putAll(injectedProps)
    new ProducerConfig(producerProps)
  }
}
