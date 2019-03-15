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


import java.util
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.Properties

import com.google.common.collect.ImmutableMap
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.samza.SamzaException
import org.apache.samza.config.ApplicationConfig.ApplicationMode
import org.apache.samza.util.{Logging, StreamUtil}

import scala.collection.JavaConverters._

object KafkaConfig {
  val TOPIC_REPLICATION_FACTOR = "replication.factor"
  val TOPIC_DEFAULT_REPLICATION_FACTOR = "2"


  val SEGMENT_BYTES = "segment.bytes"

  val CHECKPOINT_SYSTEM = "task.checkpoint.system"
  val CHECKPOINT_REPLICATION_FACTOR = "task.checkpoint." + TOPIC_REPLICATION_FACTOR
  val CHECKPOINT_SEGMENT_BYTES = "task.checkpoint." + SEGMENT_BYTES

  val CHANGELOG_STREAM_REPLICATION_FACTOR = "stores.%s.changelog." + TOPIC_REPLICATION_FACTOR
  val DEFAULT_CHANGELOG_STREAM_REPLICATION_FACTOR = CHANGELOG_STREAM_REPLICATION_FACTOR format "default"
  val CHANGELOG_STREAM_KAFKA_SETTINGS = "stores.%s.changelog.kafka."
  // The default segment size to use for changelog topics
  val CHANGELOG_DEFAULT_SEGMENT_SIZE = "536870912"

  // Helper regular expression definitions to extract/match configurations
  val CHANGELOG_STREAM_NAMES_REGEX = "stores\\.(.*)\\.changelog$"

  val JOB_COORDINATOR_REPLICATION_FACTOR = "job.coordinator." + TOPIC_REPLICATION_FACTOR
  val JOB_COORDINATOR_SEGMENT_BYTES = "job.coordinator." + SEGMENT_BYTES

  val CONSUMER_CONFIGS_CONFIG_KEY = "systems.%s.consumer.%s"
  val PRODUCER_BOOTSTRAP_SERVERS_CONFIG_KEY = "systems.%s.producer.bootstrap.servers"
  val PRODUCER_CONFIGS_CONFIG_KEY = "systems.%s.producer.%s"
  val CONSUMER_ZK_CONNECT_CONFIG_KEY = "systems.%s.consumer.zookeeper.connect"

  /**
    * Defines how low a queue can get for a single system/stream/partition
    * combination before trying to fetch more messages for it.
    */
  val CONSUMER_FETCH_THRESHOLD = SystemConfig.SYSTEM_ID_PREFIX + "samza.fetch.threshold"

  val DEFAULT_CHECKPOINT_SEGMENT_BYTES = 26214400

  /**
    * Defines how many bytes to use for the buffered prefetch messages for job as a whole.
    * The bytes for a single system/stream/partition are computed based on this.
    * This fetches wholes messages, hence this bytes limit is a soft one, and the actual usage can be
    * the bytes limit + size of max message in the partition for a given stream.
    * If the value of this property is > 0 then this takes precedence over CONSUMER_FETCH_THRESHOLD config.
    */
  val CONSUMER_FETCH_THRESHOLD_BYTES = SystemConfig.SYSTEM_ID_PREFIX + "samza.fetch.threshold.bytes"

  val DEFAULT_RETENTION_MS_FOR_BATCH = TimeUnit.DAYS.toMillis(1)

  implicit def Config2Kafka(config: Config) = new KafkaConfig(config)
}

class KafkaConfig(config: Config) extends ScalaMapConfig(config) {
  /**
    * Gets the System to use for reading/writing checkpoints. Uses the following precedence.
    *
    * 1. If task.checkpoint.system is defined, that value is used.
    * 2. If job.default.system is defined, that value is used.
    * 3. None
    */
  def getCheckpointSystem = Option(getOrElse(KafkaConfig.CHECKPOINT_SYSTEM, new JobConfig(config).getDefaultSystem.orNull))

  /**
    * Gets the replication factor for the checkpoint topic. Uses the following precedence.
    *
    * 1. If task.checkpoint.replication.factor is configured, that value is used.
    * 2. If systems.checkpoint-system.default.stream.replication.factor is configured, that value is used.
    * 3. None
    *
    * Note that the checkpoint-system has a similar precedence. See [[getCheckpointSystem]]
    */
  def getCheckpointReplicationFactor() = {
    val defaultReplicationFactor: String = getSystemDefaultReplicationFactor(getCheckpointSystem.orNull, "3")
    val replicationFactor = getOrDefault(KafkaConfig.CHECKPOINT_REPLICATION_FACTOR, defaultReplicationFactor)

    Option(replicationFactor)
  }

  private def getSystemDefaultReplicationFactor(systemName: String, defaultValue: String) = {
    val defaultReplicationFactor = new SystemConfig(config).getDefaultStreamProperties(systemName).getOrDefault(KafkaConfig.TOPIC_REPLICATION_FACTOR, defaultValue)
    defaultReplicationFactor
  }

  /**
    * Gets the segment bytes for the checkpoint topic. Uses the following precedence.
    *
    * 1. If task.checkpoint.segment.bytes is configured, that value is used.
    * 2. If systems.checkpoint-system.default.stream.segment.bytes is configured, that value is used.
    * 3. None
    *
    * Note that the checkpoint-system has a similar precedence. See [[getCheckpointSystem]]
    */
  def getCheckpointSegmentBytes() = {
    val defaultsegBytes = new SystemConfig(config).getDefaultStreamProperties(getCheckpointSystem.orNull).getInt(KafkaConfig.SEGMENT_BYTES, KafkaConfig.DEFAULT_CHECKPOINT_SEGMENT_BYTES)
    getInt(KafkaConfig.CHECKPOINT_SEGMENT_BYTES, defaultsegBytes)
  }

  /**
    * Gets the replication factor for the coordinator topic. Uses the following precedence.
    *
    * 1. If job.coordinator.replication.factor is configured, that value is used.
    * 2. If systems.coordinator-system.default.stream.replication.factor is configured, that value is used.
    * 3. 3
    *
    * Note that the coordinator-system has a similar precedence. See [[JobConfig.getCoordinatorSystemName]]
    */
  def getCoordinatorReplicationFactor = getOption(KafkaConfig.JOB_COORDINATOR_REPLICATION_FACTOR) match {
    case Some(rplFactor) => rplFactor
    case _ =>
      val coordinatorSystem = new JobConfig(config).getCoordinatorSystemNameOrNull
      val systemReplicationFactor = new SystemConfig(config).getDefaultStreamProperties(coordinatorSystem).getOrDefault(KafkaConfig.TOPIC_REPLICATION_FACTOR, "3")
      systemReplicationFactor
  }

  /**
    * Gets the segment bytes for the coordinator topic. Uses the following precedence.
    *
    * 1. If job.coordinator.segment.bytes is configured, that value is used.
    * 2. If systems.coordinator-system.default.stream.segment.bytes is configured, that value is used.
    * 3. None
    *
    * Note that the coordinator-system has a similar precedence. See [[JobConfig.getCoordinatorSystemName]]
    */
  def getCoordinatorSegmentBytes = getOption(KafkaConfig.JOB_COORDINATOR_SEGMENT_BYTES) match {
    case Some(segBytes) => segBytes
    case _ =>
      val coordinatorSystem = new JobConfig(config).getCoordinatorSystemNameOrNull
      val segBytes = new SystemConfig(config).getDefaultStreamProperties(coordinatorSystem).getOrDefault(KafkaConfig.SEGMENT_BYTES, "26214400")
      segBytes
  }

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
      .asScala
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
      .asScala
      .filterKeys(k => k.endsWith(".consumer.auto.offset.reset"))
      .map {
        case (topicAutoOffsetReset, resetValue) =>
          (topicAutoOffsetReset.replace(".consumer.auto.offset.reset", ""), resetValue)
      }.toMap
  }

  // regex resolver
  def getRegexResolvedStreams(rewriterName: String) = getOption(JobConfig.REGEX_RESOLVED_STREAMS format rewriterName)

  def getRegexResolvedSystem(rewriterName: String) = getOption(JobConfig.REGEX_RESOLVED_SYSTEM format rewriterName)

  def getRegexResolvedInheritedConfig(rewriterName: String) = config.subset((JobConfig.REGEX_INHERITED_CONFIG format rewriterName) + ".", true)

  /**
    * Gets the replication factor for the changelog topics. Uses the following precedence.
    *
    * 1. If stores.myStore.changelog.replication.factor is configured, that value is used.
    * 2. If systems.changelog-system.default.stream.replication.factor is configured, that value is used.
    * 3. 2
    *
    * Note that the changelog-system has a similar precedence. See [[JavaStorageConfig]]
    */
  def getChangelogStreamReplicationFactor(name: String) = getOption(KafkaConfig.CHANGELOG_STREAM_REPLICATION_FACTOR format name).getOrElse(getDefaultChangelogStreamReplicationFactor)

  def getDefaultChangelogStreamReplicationFactor() = {
    val changelogSystem =  new JavaStorageConfig(config).getChangelogSystem()
    getOption(KafkaConfig.DEFAULT_CHANGELOG_STREAM_REPLICATION_FACTOR).getOrElse(getSystemDefaultReplicationFactor(changelogSystem, "2"))
  }

  // The method returns a map of storenames to changelog topic names, which are configured to use kafka as the changelog stream
  def getKafkaChangelogEnabledStores() = {
    val changelogConfigs = config.regexSubset(KafkaConfig.CHANGELOG_STREAM_NAMES_REGEX).asScala
    var storeToChangelog = Map[String, String]()
    val storageConfig = new StorageConfig(config)
    val pattern = Pattern.compile(KafkaConfig.CHANGELOG_STREAM_NAMES_REGEX)

    for ((changelogConfig, cn) <- changelogConfigs) {
      // Lookup the factory for this particular stream and verify if it's a kafka system

      val matcher = pattern.matcher(changelogConfig)
      val storeName = if (matcher.find()) matcher.group(1) else throw new SamzaException("Unable to find store name in the changelog configuration: " + changelogConfig + " with SystemStream: " + cn)

      storageConfig.getChangelogStream(storeName).foreach(changelogName => {
        val systemStream = StreamUtil.getSystemStreamFromNames(changelogName)
        storeToChangelog += storeName -> systemStream.getStream
      })
    }
    storeToChangelog
  }

  // Get all kafka properties for changelog stream topic creation
  def getChangelogKafkaProperties(name: String) = {
    val filteredConfigs = config.subset(KafkaConfig.CHANGELOG_STREAM_KAFKA_SETTINGS format name, true)
    val kafkaChangeLogProperties = new Properties

    val appConfig = new ApplicationConfig(config)
    // SAMZA-1600: do not use the combination of "compact,delete" as cleanup policy until we pick up Kafka broker 0.11.0.57,
    // 1.0.2, or 1.1.0 (see KAFKA-6568)

    // Adjust changelog topic setting, when TTL is set on a RocksDB store
    //  - Disable log compaction on Kafka changelog topic
    //  - Set topic TTL to be the same as RocksDB TTL
    Option(config.get("stores.%s.rocksdb.ttl.ms" format name)) match {
      case Some(rocksDbTtl) =>
        if (!config.containsKey("stores.%s.changelog.kafka.cleanup.policy" format name)) {
          kafkaChangeLogProperties.setProperty("cleanup.policy", "delete")
          if (!config.containsKey("stores.%s.changelog.kafka.retention.ms" format name)) {
            kafkaChangeLogProperties.setProperty("retention.ms", String.valueOf(rocksDbTtl))
          }
        }
      case _ =>
        kafkaChangeLogProperties.setProperty("cleanup.policy", "compact")
    }

    kafkaChangeLogProperties.setProperty("segment.bytes", KafkaConfig.CHANGELOG_DEFAULT_SEGMENT_SIZE)
    kafkaChangeLogProperties.setProperty("delete.retention.ms", String.valueOf(new StorageConfig(config).getChangeLogDeleteRetentionInMs(name)))
    filteredConfigs.asScala.foreach { kv => kafkaChangeLogProperties.setProperty(kv._1, kv._2) }
    kafkaChangeLogProperties
  }

  // Set the checkpoint topic configs to have a very small segment size and
  // enable log compaction. This keeps job startup time small since there
  // are fewer useless (overwritten) messages to read from the checkpoint
  // topic.
  def getCheckpointTopicProperties() = {
    val segmentBytes: Int = getCheckpointSegmentBytes()
    val appConfig = new ApplicationConfig(config)
    val isStreamMode = appConfig.getAppMode == ApplicationMode.STREAM
    val properties = new Properties()

    if (isStreamMode) {
      properties.putAll(ImmutableMap.of(
        "cleanup.policy", "compact",
        "segment.bytes", String.valueOf(segmentBytes)))
    } else {
      properties.putAll(ImmutableMap.of(
        "cleanup.policy", "compact,delete",
        "retention.ms", String.valueOf(KafkaConfig.DEFAULT_RETENTION_MS_FOR_BATCH),
        "segment.bytes", String.valueOf(segmentBytes)))
    }
    properties
  }

  def getKafkaSystemProducerConfig( systemName: String,
                                    clientId: String,
                                    injectedProps: Map[String, String] = Map()) = {

    val subConf = config.subset("systems.%s.producer." format systemName, true)
    val producerProps = new util.HashMap[String, String]()
    producerProps.putAll(subConf)
    producerProps.put("client.id", clientId)
    producerProps.putAll(injectedProps.asJava)
    new KafkaProducerConfig(systemName, clientId, producerProps)
  }
}

class KafkaProducerConfig(val systemName: String,
                          val clientId: String = "",
                          properties: java.util.Map[String, String] = new util.HashMap[String, String]()) extends Logging {

  // Copied from new Kafka API - Workaround until KAFKA-1794 is resolved
  val RECONNECT_BACKOFF_MS_DEFAULT = 10L

  //Overrides specific to samza-kafka (these are considered as defaults in Samza & can be overridden by user
  val MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT: java.lang.Integer = 1.asInstanceOf[Integer]
  val RETRIES_DEFAULT: java.lang.Integer = Integer.MAX_VALUE
  val LINGER_MS_DEFAULT: java.lang.Integer = 10

  def getProducerProperties = {

    val byteArraySerializerClassName = classOf[ByteArraySerializer].getCanonicalName
    val producerProperties: java.util.Map[String, Object] = new util.HashMap[String, Object]()
    producerProperties.putAll(properties)

    if (!producerProperties.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
      debug("%s undefined. Defaulting to %s." format(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, byteArraySerializerClassName))
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, byteArraySerializerClassName)
    }

    if (!producerProperties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
      debug("%s undefined. Defaulting to %s." format(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, byteArraySerializerClassName))
      producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, byteArraySerializerClassName)
    }

    if (producerProperties.containsKey(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
      && producerProperties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).asInstanceOf[String].toInt > MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT) {
      warn("Setting '%s' to a value other than %d does not guarantee message ordering because new messages will be sent without waiting for previous ones to be acknowledged."
        format(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT))
    } else {
      producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT)
    }

    if (!producerProperties.containsKey(ProducerConfig.RETRIES_CONFIG)) {
      debug("%s undefined. Defaulting to %s." format(ProducerConfig.RETRIES_CONFIG, RETRIES_DEFAULT))
      producerProperties.put(ProducerConfig.RETRIES_CONFIG, RETRIES_DEFAULT)
    }
    producerProperties.get(ProducerConfig.RETRIES_CONFIG).toString.toInt // Verify int

    if (!producerProperties.containsKey(ProducerConfig.LINGER_MS_CONFIG)) {
      debug("%s undefined. Defaulting to %s." format(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_DEFAULT))
      producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_DEFAULT)
    }
    producerProperties.get(ProducerConfig.LINGER_MS_CONFIG).toString.toInt // Verify int

    producerProperties
  }

  val reconnectIntervalMs = Option(properties.get(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG))
    .getOrElse(RECONNECT_BACKOFF_MS_DEFAULT).asInstanceOf[Long]

  val bootsrapServers = {
    if (properties.containsKey("metadata.broker.list"))
      warn("Kafka producer configuration contains 'metadata.broker.list'. This configuration is deprecated . Samza has been upgraded " +
        "to use Kafka's new producer API. Please update your configurations based on the documentation at http://kafka.apache.org/documentation.html#newproducerconfigs")
    Option(properties.get("bootstrap.servers"))
      .getOrElse(throw new SamzaException("No bootstrap servers defined in config for %s." format systemName))
      .asInstanceOf[String]
  }
}
