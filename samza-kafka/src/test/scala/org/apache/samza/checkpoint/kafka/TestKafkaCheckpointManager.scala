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

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.utils.{CoreUtils, TestUtils}
import com.google.common.collect.ImmutableMap
import org.apache.samza.checkpoint.{Checkpoint, CheckpointId, CheckpointV1, CheckpointV2}
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.serializers.CheckpointV1Serde
import org.apache.samza.system._
import org.apache.samza.system.kafka.{KafkaStreamSpec, KafkaSystemFactory}
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals
import org.apache.samza.util.{NoOpMetricsRegistry, ReflectionUtil}
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit._
import org.mockito.Mockito
import org.mockito.Matchers

class TestKafkaCheckpointManager extends KafkaServerTestHarness {

  protected def numBrokers: Int = 3

  val checkpointSystemName = "kafka"
  val sspGrouperFactoryName = classOf[GroupByPartitionFactory].getCanonicalName

  val ssp = new SystemStreamPartition("kafka", "topic", new Partition(0))
  val checkpoint1 = new CheckpointV1(ImmutableMap.of(ssp, "offset-1"))
  val checkpoint2 = new CheckpointV1(ImmutableMap.of(ssp, "offset-2"))
  val taskName = new TaskName("Partition 0")
  var config: Config = null

  @Before
  override def setUp {
    super.setUp
    TestUtils.waitUntilTrue(() => servers.head.metadataCache.getAliveBrokers.size == numBrokers, "Wait for cache to update")
    config = getConfig()
  }

  override def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, enableControlledShutdown = true)
    // do not use relative imports
    props.map(_root_.kafka.server.KafkaConfig.fromProps)
  }

  @Test
  def testWriteCheckpointShouldRecreateSystemProducerOnFailure(): Unit = {
    val checkpointTopic = "checkpoint-topic-2"
    val mockKafkaProducer: SystemProducer = Mockito.mock(classOf[SystemProducer])

    class MockSystemFactory extends KafkaSystemFactory {
      override def getProducer(systemName: String, config: Config, registry: MetricsRegistry): SystemProducer = {
        mockKafkaProducer
      }
    }

    Mockito.doThrow(new RuntimeException()).when(mockKafkaProducer).flush(taskName.getTaskName)

    val props = new org.apache.samza.config.KafkaConfig(config).getCheckpointTopicProperties()
    val spec = new KafkaStreamSpec("id", checkpointTopic, checkpointSystemName, 1, 1, props)
    val checkPointManager = Mockito.spy(new KafkaCheckpointManager(spec, new MockSystemFactory, false, config, new NoOpMetricsRegistry))
    val newKafkaProducer: SystemProducer = Mockito.mock(classOf[SystemProducer])

    Mockito.doReturn(newKafkaProducer).when(checkPointManager).getSystemProducer()

    checkPointManager.register(taskName)
    checkPointManager.start
    checkPointManager.writeCheckpoint(taskName, new CheckpointV1(ImmutableMap.of()))
    checkPointManager.stop()

    // Verifications after the test

    Mockito.verify(mockKafkaProducer).stop()
    Mockito.verify(newKafkaProducer).register(taskName.getTaskName)
    Mockito.verify(newKafkaProducer).start()
  }

  @Test
  def testCheckpointShouldBeNullIfCheckpointTopicDoesNotExistShouldBeCreatedOnWriteAndShouldBeReadableAfterWrite(): Unit = {
    val checkpointTopic = "checkpoint-topic-1"
    val kcm1 = createKafkaCheckpointManager(checkpointTopic)
    kcm1.register(taskName)
    kcm1.createResources
    kcm1.start
    kcm1.stop

    // check that start actually creates the topic with log compaction enabled
    val topicConfig = adminZkClient.getAllTopicConfigs().getOrElse(checkpointTopic, new Properties())

    assertEquals(topicConfig, new KafkaConfig(config).getCheckpointTopicProperties())
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))

    // read before topic exists should result in a null checkpoint
    val readCp = readCheckpoint(checkpointTopic, taskName)
    assertNull(readCp)

    writeCheckpoint(checkpointTopic, taskName, checkpoint1)
    assertEquals(checkpoint1, readCheckpoint(checkpointTopic, taskName))

    // writing a second message and reading it returns a more recent checkpoint
    writeCheckpoint(checkpointTopic, taskName, checkpoint2)
    assertEquals(checkpoint2, readCheckpoint(checkpointTopic, taskName))
  }

  @Test
  def testCheckpointV1AndV2WriteAndReadV1(): Unit = {
    val checkpointTopic = "checkpoint-topic-1"
    val kcm1 = createKafkaCheckpointManager(checkpointTopic)
    kcm1.register(taskName)
    kcm1.createResources
    kcm1.start
    kcm1.stop

    // check that start actually creates the topic with log compaction enabled
    val topicConfig = adminZkClient.getAllTopicConfigs().getOrElse(checkpointTopic, new Properties())

    assertEquals(topicConfig, new KafkaConfig(config).getCheckpointTopicProperties())
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))

    // read before topic exists should result in a null checkpoint
    val readCp = readCheckpoint(checkpointTopic, taskName)
    assertNull(readCp)

    val checkpointV1 = new CheckpointV1(ImmutableMap.of(ssp, "offset-1"))
    val checkpointV2 = new CheckpointV2(CheckpointId.create(), ImmutableMap.of(ssp, "offset-2"),
      ImmutableMap.of("factory1", ImmutableMap.of("store1", "changelogOffset")))

    // skips v2 checkpoints from checkpoint topic
    writeCheckpoint(checkpointTopic, taskName, checkpointV2)
    assertNull(readCheckpoint(checkpointTopic, taskName))

    // reads latest v1 checkpoints
    writeCheckpoint(checkpointTopic, taskName, checkpointV1)
    assertEquals(checkpointV1, readCheckpoint(checkpointTopic, taskName))

    // writing checkpoint v2 still returns the previous v1 checkpoint
    writeCheckpoint(checkpointTopic, taskName, checkpointV2)
    assertEquals(checkpointV1, readCheckpoint(checkpointTopic, taskName))
  }

  @Test
  def testCheckpointV1AndV2WriteAndReadV2(): Unit = {
    val checkpointTopic = "checkpoint-topic-1"
    val kcm1 = createKafkaCheckpointManager(checkpointTopic)
    kcm1.register(taskName)
    kcm1.createResources
    kcm1.start
    kcm1.stop

    // check that start actually creates the topic with log compaction enabled
    val topicConfig = adminZkClient.getAllTopicConfigs().getOrElse(checkpointTopic, new Properties())

    assertEquals(topicConfig, new KafkaConfig(config).getCheckpointTopicProperties())
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))

    // read before topic exists should result in a null checkpoint
    val readCp = readCheckpoint(checkpointTopic, taskName)
    assertNull(readCp)

    val checkpointV1 = new CheckpointV1(ImmutableMap.of(ssp, "offset-1"))
    val checkpointV2 = new CheckpointV2(CheckpointId.create(), ImmutableMap.of(ssp, "offset-2"),
      ImmutableMap.of("factory1", ImmutableMap.of("store1", "changelogOffset")))

    val overrideConfig = new MapConfig(new ImmutableMap.Builder[String, String]()
      .put(JobConfig.JOB_NAME, "some-job-name")
      .put(JobConfig.JOB_ID, "i001")
      .put(s"systems.$checkpointSystemName.samza.factory", classOf[KafkaSystemFactory].getCanonicalName)
      .put(s"systems.$checkpointSystemName.producer.bootstrap.servers", brokerList)
      .put(s"systems.$checkpointSystemName.consumer.zookeeper.connect", zkConnect)
      .put("task.checkpoint.system", checkpointSystemName)
      .put(TaskConfig.CHECKPOINT_READ_VERSION, "2")
      .build())

    // Skips reading any v1 checkpoints
    writeCheckpoint(checkpointTopic, taskName, checkpointV1)
    assertNull(readCheckpoint(checkpointTopic, taskName, overrideConfig))

    // writing a v2 checkpoint would allow reading it back
    writeCheckpoint(checkpointTopic, taskName, checkpointV2)
    assertEquals(checkpointV2, readCheckpoint(checkpointTopic, taskName, overrideConfig))

    // writing v1 checkpoint is still skipped
    writeCheckpoint(checkpointTopic, taskName, checkpointV1)
    assertEquals(checkpointV2, readCheckpoint(checkpointTopic, taskName, overrideConfig))
  }

  @Test
  def testWriteCheckpointShouldRetryFiniteTimesOnFailure(): Unit = {
    val checkpointTopic = "checkpoint-topic-2"
    val mockKafkaProducer: SystemProducer = Mockito.mock(classOf[SystemProducer])
    val mockKafkaSystemConsumer: SystemConsumer = Mockito.mock(classOf[SystemConsumer])

    Mockito.doThrow(new RuntimeException()).when(mockKafkaProducer).flush(taskName.getTaskName)

    val props = new org.apache.samza.config.KafkaConfig(config).getCheckpointTopicProperties()
    val spec = new KafkaStreamSpec("id", checkpointTopic, checkpointSystemName, 1, 1, props)
    val checkPointManager = new KafkaCheckpointManager(spec, new MockSystemFactory(mockKafkaSystemConsumer, mockKafkaProducer), false, config, new NoOpMetricsRegistry)
    checkPointManager.MaxRetryDurationInMillis = 1

    try {
      checkPointManager.register(taskName)
      checkPointManager.start
      checkPointManager.writeCheckpoint(taskName, new CheckpointV1(ImmutableMap.of()))
    } catch {
      case _: SamzaException => info("Got SamzaException as expected.")
      case unexpectedException: Throwable => fail("Expected SamzaException but got %s" format unexpectedException)
    } finally {
      checkPointManager.stop()
    }
  }

  @Test
  def testFailOnTopicValidation(): Unit = {
    // By default, should fail if there is a topic validation error
    val checkpointTopic = "eight-partition-topic";
    val kcm = createKafkaCheckpointManager(checkpointTopic)
    kcm.register(taskName)
    // create topic with the wrong number of partitions
    createTopic(checkpointTopic, 8, new KafkaConfig(config).getCheckpointTopicProperties())
    try {
      kcm.createResources()
      kcm.start()
      fail("Expected an exception for invalid number of partitions in the checkpoint topic.")
    } catch {
      case e: StreamValidationException => None
    }
    kcm.stop()
  }

  @Test
  def testNoFailOnTopicValidationDisabled(): Unit = {
    val checkpointTopic = "eight-partition-topic";
    // create topic with the wrong number of partitions
    createTopic(checkpointTopic, 8, new KafkaConfig(config).getCheckpointTopicProperties())
    val failOnTopicValidation = false
    val kcm = createKafkaCheckpointManager(checkpointTopic, new CheckpointV1Serde, failOnTopicValidation)
    kcm.register(taskName)
    kcm.createResources()
    kcm.start()
    kcm.stop()
  }

  @Test
  def testConsumerStopsAfterInitialReadIfConfigSetTrue(): Unit = {
    val mockKafkaSystemConsumer: SystemConsumer = Mockito.mock(classOf[SystemConsumer])

    val checkpointTopic = "checkpoint-topic-test"
    val props = new org.apache.samza.config.KafkaConfig(config).getCheckpointTopicProperties()
    val spec = new KafkaStreamSpec("id", checkpointTopic, checkpointSystemName, 1, 1, props)

    val configMapWithOverride = new java.util.HashMap[String, String](config)
    configMapWithOverride.put(TaskConfig.INTERNAL_CHECKPOINT_MANAGER_CONSUMER_STOP_AFTER_FIRST_READ, "true")
    val kafkaCheckpointManager = new KafkaCheckpointManager(spec, new MockSystemFactory(mockKafkaSystemConsumer), false, new MapConfig(configMapWithOverride), new NoOpMetricsRegistry)

    kafkaCheckpointManager.register(taskName)
    kafkaCheckpointManager.start()
    kafkaCheckpointManager.readLastCheckpoint(taskName)

    Mockito.verify(mockKafkaSystemConsumer, Mockito.times(1)).register(Matchers.any(), Matchers.any())
    Mockito.verify(mockKafkaSystemConsumer, Mockito.times(1)).start()
    Mockito.verify(mockKafkaSystemConsumer, Mockito.times(1)).poll(Matchers.any(), Matchers.any())
    Mockito.verify(mockKafkaSystemConsumer, Mockito.times(1)).stop()

    kafkaCheckpointManager.stop()

    Mockito.verifyNoMoreInteractions(mockKafkaSystemConsumer)
  }

  @Test
  def testConsumerDoesNotStopAfterInitialReadIfConfigSetFalse(): Unit = {
    val mockKafkaSystemConsumer: SystemConsumer = Mockito.mock(classOf[SystemConsumer])

    val checkpointTopic = "checkpoint-topic-test"
    val props = new org.apache.samza.config.KafkaConfig(config).getCheckpointTopicProperties()
    val spec = new KafkaStreamSpec("id", checkpointTopic, checkpointSystemName, 1, 1, props)

    val configMapWithOverride = new java.util.HashMap[String, String](config)
    configMapWithOverride.put(TaskConfig.INTERNAL_CHECKPOINT_MANAGER_CONSUMER_STOP_AFTER_FIRST_READ, "false")
    val kafkaCheckpointManager = new KafkaCheckpointManager(spec, new MockSystemFactory(mockKafkaSystemConsumer), false, new MapConfig(configMapWithOverride), new NoOpMetricsRegistry)

    kafkaCheckpointManager.register(taskName)
    kafkaCheckpointManager.start()
    kafkaCheckpointManager.readLastCheckpoint(taskName)

    Mockito.verify(mockKafkaSystemConsumer, Mockito.times(0)).stop()

    kafkaCheckpointManager.stop()

    Mockito.verify(mockKafkaSystemConsumer, Mockito.times(1)).stop()
  }

  @After
  override def tearDown(): Unit = {
    if (servers != null) {
      servers.foreach(_.shutdown())
      servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    }
    super.tearDown
  }

  private def getConfig(): Config = {
    new MapConfig(new ImmutableMap.Builder[String, String]()
      .put(JobConfig.JOB_NAME, "some-job-name")
      .put(JobConfig.JOB_ID, "i001")
      .put(s"systems.$checkpointSystemName.samza.factory", classOf[KafkaSystemFactory].getCanonicalName)
      .put(s"systems.$checkpointSystemName.producer.bootstrap.servers", brokerList)
      .put(s"systems.$checkpointSystemName.consumer.zookeeper.connect", zkConnect)
      .put("task.checkpoint.system", checkpointSystemName)
      .build())
  }

  private def createKafkaCheckpointManager(cpTopic: String, serde: CheckpointV1Serde = new CheckpointV1Serde,
    failOnTopicValidation: Boolean = true, overrideConfig: Config = config) = {
    val kafkaConfig = new org.apache.samza.config.KafkaConfig(overrideConfig)
    val props = kafkaConfig.getCheckpointTopicProperties()
    val systemName = kafkaConfig.getCheckpointSystem.getOrElse(
      throw new SamzaException("No system defined for Kafka's checkpoint manager."))

    val systemConfig = new SystemConfig(overrideConfig)
    val systemFactoryClassName = JavaOptionals.toRichOptional(systemConfig.getSystemFactory(systemName)).toOption
      .getOrElse(throw new SamzaException("Missing configuration: " + SystemConfig.SYSTEM_FACTORY_FORMAT format systemName))

    val systemFactory = ReflectionUtil.getObj(systemFactoryClassName, classOf[SystemFactory])

    val spec = new KafkaStreamSpec("id", cpTopic, checkpointSystemName, 1, 1, props)
    new KafkaCheckpointManager(spec, systemFactory, failOnTopicValidation, overrideConfig, new NoOpMetricsRegistry, serde)
  }

  private def readCheckpoint(checkpointTopic: String, taskName: TaskName, config: Config = config) : Checkpoint = {
    val kcm = createKafkaCheckpointManager(checkpointTopic, overrideConfig = config)
    kcm.register(taskName)
    kcm.start
    val checkpoint = kcm.readLastCheckpoint(taskName)
    kcm.stop
    checkpoint
  }

  private def writeCheckpoint(checkpointTopic: String, taskName: TaskName, checkpoint: Checkpoint): Unit = {
    val kcm = createKafkaCheckpointManager(checkpointTopic)
    kcm.register(taskName)
    kcm.start
    kcm.writeCheckpoint(taskName, checkpoint)
    kcm.stop
  }

  private def createTopic(cpTopic: String, partNum: Int, props: Properties) {
    adminZkClient.createTopic(cpTopic, partNum, 1, props)
  }

  class MockSystemFactory(
    mockKafkaSystemConsumer: SystemConsumer = Mockito.mock(classOf[SystemConsumer]),
    mockKafkaProducer: SystemProducer = Mockito.mock(classOf[SystemProducer])) extends KafkaSystemFactory {
    override def getProducer(systemName: String, config: Config, registry: MetricsRegistry): SystemProducer = {
      mockKafkaProducer
    }

    override def getConsumer(systemName: String, config: Config, registry: MetricsRegistry): SystemConsumer = {
      mockKafkaSystemConsumer
    }
  }

}
