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

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.junit.Assert._
import org.junit.After
import org.junit.Before
import org.junit.Test

import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.clients.producer.ProducerConfig

class TestKafkaConfig {

  var props : Properties = new Properties
  val SYSTEM_NAME = "kafka"
  val KAFKA_PRODUCER_PROPERTY_PREFIX = "systems." + SYSTEM_NAME + ".producer."
  val TEST_CLIENT_ID = "TestClientId"
  val TEST_GROUP_ID = "TestGroupId"

  @Before
  def setupProperties() {
    props = new Properties
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + "bootstrap.servers", "localhost:9092")
    props.setProperty("systems." + SYSTEM_NAME + ".consumer.zookeeper.connect", "localhost:2181/")
    props.setProperty(JobConfig.JOB_NAME, "jobName")
  }

  @After
  def clearUpProperties(): Unit = {
    props.clear()
  }

  @Test
  def testStreamLevelFetchSizeOverride() {
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val consumerConfig = KafkaConsumerConfig.getKafkaSystemConsumerConfig(mapConfig, SYSTEM_NAME, TEST_CLIENT_ID)
    // default fetch size
    assertEquals(1024*1024, consumerConfig.fetchMessageMaxBytes)

    props.setProperty("systems." + SYSTEM_NAME + ".consumer.fetch.message.max.bytes", "262144")
    val mapConfig1 = new MapConfig(props.asScala.asJava)
    val kafkaConfig1 = new KafkaConfig(mapConfig1)
    val consumerConfig1 = KafkaConsumerConfig.getKafkaSystemConsumerConfig(mapConfig1, SYSTEM_NAME, TEST_CLIENT_ID)
    // shared fetch size
    assertEquals(512*512, consumerConfig1.fetchMessageMaxBytes)

    props.setProperty("systems." + SYSTEM_NAME + ".streams.topic1.consumer.fetch.message.max.bytes", "65536")
    val mapConfig2 = new MapConfig(props.asScala.asJava)
    val kafkaConfig2 = new KafkaConfig(mapConfig2)
    val consumerConfig2 = kafkaConfig2.getFetchMessageMaxBytesTopics(SYSTEM_NAME)
    // topic fetch size
    assertEquals(256*256, consumerConfig2 getOrElse ("topic1", 1024*1024))

    // default samza.fetch.threshold.bytes
    val mapConfig3 = new MapConfig(props.asScala.asJava)
    val kafkaConfig3 = new KafkaConfig(mapConfig3)
    assertTrue(kafkaConfig3.getConsumerFetchThresholdBytes("kafka").isEmpty)

    props.setProperty("systems.kafka.samza.fetch.threshold.bytes", "65536")
    val mapConfig4 = new MapConfig(props.asScala.asJava)
    val kafkaConfig4 = new KafkaConfig(mapConfig4)
    assertEquals("65536", kafkaConfig4.getConsumerFetchThresholdBytes("kafka").get)
  }

  @Test
  def testChangeLogPropertiesShouldReturnCorrectTopicConfigurationForInfiniteTTLStores(): Unit = {
    val props = new Properties
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + "bootstrap.servers", "localhost:9092")
    props.setProperty("systems." + SYSTEM_NAME + ".consumer.zookeeper.connect", "localhost:2181/")
    props.setProperty(JobConfig.JOB_NAME, "jobName")

    props.setProperty("stores.test1.changelog", "kafka.mychangelog1")
    props.setProperty("stores.test1.rocksdb.ttl.ms", "-1")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProperties = kafkaConfig.getChangelogKafkaProperties("test1")
    assertEquals("compact", kafkaProperties.getProperty("cleanup.policy"))
    assertEquals("536870912", kafkaProperties.getProperty("segment.bytes"))
    assertEquals("1000012", kafkaProperties.getProperty("max.message.bytes"))
    assertEquals("86400000", kafkaProperties.getProperty("delete.retention.ms"))
  }

  @Test
  def testChangeLogPropertiesShouldReturnCorrectTopicConfigurationForLargeTTLStores(): Unit = {
    val props = new Properties
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + "bootstrap.servers", "localhost:9092")
    props.setProperty("systems." + SYSTEM_NAME + ".consumer.zookeeper.connect", "localhost:2181/")
    props.setProperty(JobConfig.JOB_NAME, "jobName")

    props.setProperty("stores.test1.changelog", "kafka.mychangelog1")
    // Set the RocksDB TTL to be 28 days.
    props.setProperty("stores.test1.rocksdb.ttl.ms", "2419200000")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProperties = kafkaConfig.getChangelogKafkaProperties("test1")
    assertEquals("delete", kafkaProperties.getProperty("cleanup.policy"))
    assertEquals("536870912", kafkaProperties.getProperty("segment.bytes"))
    assertEquals("86400000", kafkaProperties.getProperty("delete.retention.ms"))
  }

  @Test
  def testChangeLogPropertiesShouldReturnCorrectTopicConfigurationForStoresWithEmptyRocksDBTTL(): Unit = {
    val props = new Properties
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + "bootstrap.servers", "localhost:9092")
    props.setProperty("systems." + SYSTEM_NAME + ".consumer.zookeeper.connect", "localhost:2181/")
    props.setProperty(JobConfig.JOB_NAME, "jobName")

    props.setProperty("stores.test1.changelog", "kafka.mychangelog1")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProperties = kafkaConfig.getChangelogKafkaProperties("test1")
    assertEquals("compact", kafkaProperties.getProperty("cleanup.policy"))
    assertEquals("536870912", kafkaProperties.getProperty("segment.bytes"))
    assertEquals("86400000", kafkaProperties.getProperty("delete.retention.ms"))
    assertEquals("1000012", kafkaProperties.getProperty("max.message.bytes"))

  }

  @Test
  def testChangeLogProperties() {
    props.setProperty("job.changelog.system", SYSTEM_NAME)
    props.setProperty("systems." + SYSTEM_NAME + ".samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory")
    props.setProperty("stores.test1.changelog", "kafka.mychangelog1")
    props.setProperty("stores.test2.changelog", "kafka.mychangelog2")
    props.setProperty("stores.test2.changelog.max.message.bytes", "1024000")
    props.setProperty("stores.test3.changelog", "otherstream")
    props.setProperty("stores.test1.changelog.kafka.cleanup.policy", "delete")
    props.setProperty("stores.test4.rocksdb.ttl.ms", "3600")
    props.setProperty("stores.test5.rocksdb.ttl.ms", "3600")
    props.setProperty("stores.test5.changelog.kafka.retention.ms", "1000")
    props.setProperty("stores.test6.rocksdb.ttl.ms", "3600")
    props.setProperty("stores.test6.changelog.kafka.cleanup.policy", "compact")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test1").getProperty("cleanup.policy"), "delete")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test2").getProperty("cleanup.policy"), "compact")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test2").getProperty("max.message.bytes"), "1024000")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test3").getProperty("cleanup.policy"), "compact")
    val storeToChangelog = kafkaConfig.getKafkaChangelogEnabledStores()
    assertEquals("mychangelog1", storeToChangelog.getOrDefault("test1", ""))
    assertEquals("mychangelog2", storeToChangelog.getOrDefault("test2", ""))
    assertEquals("otherstream", storeToChangelog.getOrDefault("test3", ""))
    assertNull(kafkaConfig.getChangelogKafkaProperties("test1").getProperty("retention.ms"))
    assertNull(kafkaConfig.getChangelogKafkaProperties("test2").getProperty("retention.ms"))
    assertNull(kafkaConfig.getChangelogKafkaProperties("test1").getProperty("min.compaction.lag.ms"))

    props.setProperty("systems." + SYSTEM_NAME + ".samza.factory", "org.apache.samza.system.kafka.SomeOtherFactory")
    val storeToChangelog1 = kafkaConfig.getKafkaChangelogEnabledStores()
    assertEquals("mychangelog1", storeToChangelog1.getOrDefault("test1", ""))
    assertEquals("mychangelog2", storeToChangelog1.getOrDefault("test2", ""))
    assertEquals("otherstream", storeToChangelog1.getOrDefault("test3", ""))

    assertEquals(kafkaConfig.getChangelogKafkaProperties("test4").getProperty("cleanup.policy"), "delete")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test4").getProperty("retention.ms"), "3600")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test4").getProperty("max.message.bytes"), null)

    assertEquals(kafkaConfig.getChangelogKafkaProperties("test5").getProperty("cleanup.policy"), "delete")
    assertEquals(kafkaConfig.getChangelogKafkaProperties("test5").getProperty("retention.ms"), "1000")

    assertEquals(kafkaConfig.getChangelogKafkaProperties("test6").getProperty("cleanup.policy"), "compact")
    assertNull(kafkaConfig.getChangelogKafkaProperties("test6").getProperty("retention.ms"))

    props.setProperty(ApplicationConfig.APP_MODE, ApplicationConfig.ApplicationMode.BATCH.name())
    val batchMapConfig = new MapConfig(props.asScala.asJava)
    val batchKafkaConfig = new KafkaConfig(batchMapConfig)
    assertEquals(batchKafkaConfig.getChangelogKafkaProperties("test1").getProperty("cleanup.policy"), "delete")
    assertEquals(batchKafkaConfig.getChangelogKafkaProperties("test1").getProperty("delete.retention.ms"),
      String.valueOf(KafkaConfig.DEFAULT_RETENTION_MS_FOR_BATCH))
    assertEquals(batchKafkaConfig.getChangelogKafkaProperties("test2").getProperty("cleanup.policy"), "compact")
    assertEquals(batchKafkaConfig.getChangelogKafkaProperties("test2").getProperty("delete.retention.ms"),
      String.valueOf(KafkaConfig.DEFAULT_RETENTION_MS_FOR_BATCH))
    assertEquals(batchKafkaConfig.getChangelogKafkaProperties("test3").getProperty("cleanup.policy"), "compact")
    assertEquals(batchKafkaConfig.getChangelogKafkaProperties("test3").getProperty("delete.retention.ms"),
      String.valueOf(KafkaConfig.DEFAULT_RETENTION_MS_FOR_BATCH))
    assertEquals(batchKafkaConfig.getChangelogKafkaProperties("test3").getProperty("max.message.bytes"),
      KafkaConfig.DEFAULT_LOG_COMPACT_TOPIC_MAX_MESSAGE_BYTES)

    // test compaction config for transactional state
    val lagOverride = String.valueOf(TimeUnit.HOURS.toMillis(6))
    props.setProperty(TaskConfig.TRANSACTIONAL_STATE_RESTORE_ENABLED, "true")
    props.setProperty("stores.test2.changelog.min.compaction.lag.ms", lagOverride)
    val tsMapConfig = new MapConfig(props.asScala.asJava)
    val tsKafkaConfig = new KafkaConfig(tsMapConfig)
    assertEquals(String.valueOf(StorageConfig.DEFAULT_CHANGELOG_MIN_COMPACTION_LAG_MS),
      tsKafkaConfig.getChangelogKafkaProperties("test1").getProperty("min.compaction.lag.ms"))
    assertEquals(lagOverride,
      tsKafkaConfig.getChangelogKafkaProperties("test2").getProperty("min.compaction.lag.ms"))
  }

  @Test
  def testDefaultValuesForProducerProperties() {
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    val producerProperties = kafkaProducerConfig.getProducerProperties

    assertEquals(classOf[ByteArraySerializer].getCanonicalName, producerProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
    assertEquals(classOf[ByteArraySerializer].getCanonicalName, producerProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
    assertEquals(kafkaProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DEFAULT, producerProperties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION))
    assertEquals(kafkaProducerConfig.RETRIES_DEFAULT, producerProperties.get(ProducerConfig.RETRIES_CONFIG))
  }

  @Test
  def testMaxInFlightRequestsPerConnectionOverride() {
    val expectedValue = "200"

    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, expectedValue)

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    val producerProperties = kafkaProducerConfig.getProducerProperties

    assertEquals(expectedValue, producerProperties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION))
  }

  @Test
  def testRetriesOverride() {
    val expectedValue = "200"

    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.RETRIES_CONFIG, expectedValue)

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    val producerProperties = kafkaProducerConfig.getProducerProperties

    assertEquals(expectedValue, producerProperties.get(ProducerConfig.RETRIES_CONFIG))
  }

  @Test(expected = classOf[NumberFormatException])
  def testMaxInFlightRequestsPerConnectionWrongNumberFormat() {
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "Samza")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    kafkaProducerConfig.getProducerProperties
  }

  @Test(expected = classOf[NumberFormatException])
  def testRetriesWrongNumberFormat() {
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.RETRIES_CONFIG, "Samza")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    kafkaProducerConfig.getProducerProperties
  }

  @Test(expected = classOf[NumberFormatException])
  def testLingerWrongNumberFormat() {
    props.setProperty(KAFKA_PRODUCER_PROPERTY_PREFIX + ProducerConfig.LINGER_MS_CONFIG, "Samza")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val kafkaProducerConfig = kafkaConfig.getKafkaSystemProducerConfig(SYSTEM_NAME, TEST_CLIENT_ID)
    kafkaProducerConfig.getProducerProperties
  }

  @Test
  def testGetSystemDefaultReplicationFactor(): Unit = {
    assertEquals(KafkaConfig.TOPIC_DEFAULT_REPLICATION_FACTOR, new KafkaConfig(new MapConfig()).getSystemDefaultReplicationFactor("kafka-system",KafkaConfig.TOPIC_DEFAULT_REPLICATION_FACTOR))

    props.setProperty("systems.kafka-system.default.stream.replication.factor", "8")
    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals("8", kafkaConfig.getSystemDefaultReplicationFactor("kafka-system","2"))
  }

  @Test
  def testChangeLogReplicationFactor() {
    props.setProperty("stores.store-with-override.changelog", "kafka-system.changelog-topic")
    props.setProperty("stores.store-with-override.changelog.replication.factor", "3")
    props.setProperty("stores.default.changelog.replication.factor", "2")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals("3", kafkaConfig.getChangelogStreamReplicationFactor("store-with-override"))
    assertEquals("2", kafkaConfig.getChangelogStreamReplicationFactor("store-without-override"))
  }

  @Test
  def testChangeLogReplicationFactorWithOverriddenDefault() {
    props.setProperty(JobConfig.JOB_DEFAULT_SYSTEM, "kafka-system")
    props.setProperty("stores.store-with-override.changelog", "changelog-topic")
    props.setProperty("stores.store-with-override.changelog.replication.factor", "4")
    // Override the "default" default value
    props.setProperty("stores.default.changelog.replication.factor", "5")


    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals("4", kafkaConfig.getChangelogStreamReplicationFactor("store-with-override"))
    assertEquals("5", kafkaConfig.getChangelogStreamReplicationFactor("store-without-override"))
  }

  @Test
  def testChangeLogReplicationFactorWithSystemOverriddenDefault() {
    props.setProperty(StorageConfig.CHANGELOG_SYSTEM, "kafka-system")
    props.setProperty("systems.kafka-system.default.stream.replication.factor", "8")
    props.setProperty("stores.store-with-override.changelog.replication.factor", "4")
    props.setProperty("stores.store-without-override.changelog", "change-for-store-without-override")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals("4", kafkaConfig.getChangelogStreamReplicationFactor("store-with-override"))
    assertEquals("8", kafkaConfig.getChangelogStreamReplicationFactor("store-without-override"))
  }

  @Test
  def testCheckpointConfigs() {
    val emptyConfig = new KafkaConfig(new MapConfig())
    assertEquals("3", emptyConfig.getCheckpointReplicationFactor.orNull)
    assertEquals(1000012, emptyConfig.getCheckpointMaxMessageBytes())
    assertNull(emptyConfig.getCheckpointSystem.orNull)

    props.setProperty(KafkaConfig.CHECKPOINT_SYSTEM, "kafka-system")
    props.setProperty("task.checkpoint.replication.factor", "4")
    props.setProperty("task.checkpoint.max.message.bytes", "2048000")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals("kafka-system", kafkaConfig.getCheckpointSystem.orNull)
    assertEquals("4", kafkaConfig.getCheckpointReplicationFactor.orNull)
    assertEquals(2048000, kafkaConfig.getCheckpointMaxMessageBytes())
  }

  @Test
  def testCheckpointConfigsWithSystemDefault() {
    props.setProperty(JobConfig.JOB_DEFAULT_SYSTEM, "other-kafka-system")
    props.setProperty("systems.other-kafka-system.default.stream.replication.factor", "8")
    props.setProperty("systems.other-kafka-system.default.stream.segment.bytes", "8675309")
    props.setProperty("systems.other-kafka-system.default.stream.max.message.bytes", "4096000")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals("other-kafka-system", kafkaConfig.getCheckpointSystem.orNull)
    assertEquals("8", kafkaConfig.getCheckpointReplicationFactor.orNull)
    assertEquals(8675309, kafkaConfig.getCheckpointSegmentBytes)
    assertEquals(4096000, kafkaConfig.getCheckpointMaxMessageBytes())
  }

  @Test
  def testCheckpointConfigsWithSystemOverriddenDefault() {
    // Defaults
    props.setProperty(JobConfig.JOB_DEFAULT_SYSTEM, "other-kafka-system")
    props.setProperty("systems.other-kafka-system.default.stream.replication.factor", "8")
    props.setProperty("systems.other-kafka-system.default.stream.max.message.bytes", "4096000")
    props.setProperty("systems.kafka-system.default.stream.segment.bytes", "8675309")

    // Overrides
    props.setProperty(KafkaConfig.CHECKPOINT_SYSTEM, "kafka-system")
    props.setProperty(KafkaConfig.CHECKPOINT_REPLICATION_FACTOR, "4")
    props.setProperty(KafkaConfig.CHECKPOINT_MAX_MESSAGE_BYTES, "8192000")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    assertEquals("kafka-system", kafkaConfig.getCheckpointSystem.orNull)
    assertEquals("4", kafkaConfig.getCheckpointReplicationFactor.orNull)
    assertEquals(8675309, kafkaConfig.getCheckpointSegmentBytes)
    assertEquals(8192000, kafkaConfig.getCheckpointMaxMessageBytes())
  }

  @Test
  def testCoordinatorConfigs() {
    val emptyConfig = new KafkaConfig(new MapConfig())
    assertEquals("3", emptyConfig.getCoordinatorReplicationFactor)
    assertEquals("1000012", emptyConfig.getCoordinatorMaxMessageByte)
    assertNull(new JobConfig(new MapConfig()).getCoordinatorSystemNameOrNull)

    props.setProperty(JobConfig.JOB_COORDINATOR_SYSTEM, "kafka-system")
    props.setProperty(KafkaConfig.JOB_COORDINATOR_REPLICATION_FACTOR, "4")
    props.setProperty(KafkaConfig.JOB_COORDINATOR_MAX_MESSAGE_BYTES, "1024000")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val jobConfig = new JobConfig(mapConfig)
    assertEquals("kafka-system", jobConfig.getCoordinatorSystemNameOrNull)
    assertEquals("4", kafkaConfig.getCoordinatorReplicationFactor)
    assertEquals("1024000", kafkaConfig.getCoordinatorMaxMessageByte)
  }

  @Test
  def testCoordinatorConfigsWithSystemDefault() {
    props.setProperty(JobConfig.JOB_DEFAULT_SYSTEM, "other-kafka-system")
    props.setProperty("systems.other-kafka-system.default.stream.replication.factor", "8")
    props.setProperty("systems.other-kafka-system.default.stream.segment.bytes", "8675309")
    props.setProperty("systems.other-kafka-system.default.stream.max.message.bytes", "2048000")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val jobConfig = new JobConfig(mapConfig)
    assertEquals("other-kafka-system", jobConfig.getCoordinatorSystemNameOrNull)
    assertEquals("8", kafkaConfig.getCoordinatorReplicationFactor)
    assertEquals("8675309", kafkaConfig.getCoordinatorSegmentBytes)
    assertEquals("2048000", kafkaConfig.getCoordinatorMaxMessageByte)
  }

  @Test
  def testCoordinatorConfigsWithSystemOverriddenDefault() {
    // Defaults
    props.setProperty(JobConfig.JOB_DEFAULT_SYSTEM, "other-kafka-system")
    props.setProperty("systems.other-kafka-system.default.stream.replication.factor", "8")
    props.setProperty("systems.other-kafka-system.default.stream.max.message.byte", "2048000")
    props.setProperty("systems.kafka-system.default.stream.segment.bytes", "8675309")

    // Overrides
    props.setProperty(JobConfig.JOB_COORDINATOR_SYSTEM, "kafka-system")
    props.setProperty(KafkaConfig.JOB_COORDINATOR_REPLICATION_FACTOR, "4")
    props.setProperty(KafkaConfig.JOB_COORDINATOR_MAX_MESSAGE_BYTES, "4096000")

    val mapConfig = new MapConfig(props.asScala.asJava)
    val kafkaConfig = new KafkaConfig(mapConfig)
    val jobConfig = new JobConfig(mapConfig)
    assertEquals("kafka-system", jobConfig.getCoordinatorSystemNameOrNull)
    assertEquals("4", kafkaConfig.getCoordinatorReplicationFactor)
    assertEquals("4096000", kafkaConfig.getCoordinatorMaxMessageByte)
    assertEquals("8675309", kafkaConfig.getCoordinatorSegmentBytes)
  }
}
