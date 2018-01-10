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

import kafka.admin.AdminUtils
import kafka.integration.KafkaServerTestHarness
import kafka.server.ConfigType
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import com.google.common.collect.ImmutableMap
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system._
import org.apache.samza.system.kafka.{KafkaStreamSpec, KafkaSystemFactory}
import org.apache.samza.util.{KafkaUtilException, NoOpMetricsRegistry, Util}
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit._

class TestKafkaCheckpointManager extends KafkaServerTestHarness {

  protected def numBrokers: Int = 3

  val checkpointSystemName = "kafka"
  val sspGrouperFactoryName = classOf[GroupByPartitionFactory].getCanonicalName

  val ssp = new SystemStreamPartition("kafka", "topic", new Partition(0))
  val checkpoint1 = new Checkpoint(ImmutableMap.of(ssp, "offset-1"))
  val checkpoint2 = new Checkpoint(ImmutableMap.of(ssp, "offset-2"))
  val taskName = new TaskName("Partition 0")
  var config: Config = null

  @Before
  override def setUp {
    super.setUp
    TestUtils.waitUntilTrue(() => servers.head.metadataCache.getAliveBrokers.size == numBrokers, "Wait for cache to update")
    config = getConfig()
  }

  override def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, true)
    // do not use relative imports
    props.map(_root_.kafka.server.KafkaConfig.fromProps)
  }

  @Test
  def testCheckpointShouldBeNullIfCheckpointTopicDoesNotExistShouldBeCreatedOnWriteAndShouldBeReadableAfterWrite {
    val checkpointTopic = "checkpoint-topic-1"
    val kcm1 = createKafkaCheckpointManager(checkpointTopic)
    kcm1.register(taskName)
    kcm1.start
    kcm1.stop
    // check that start actually creates the topic with log compaction enabled
    val zkClient = ZkUtils(zkConnect, 6000, 6000, JaasUtils.isZkSecurityEnabled())
    val topicConfig = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, checkpointTopic)

    assertEquals(topicConfig, new KafkaConfig(config).getCheckpointTopicProperties())
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))

    zkClient.close

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
  def testFailOnTopicValidation {
    // By default, should fail if there is a topic validation error
    val checkpointTopic = "eight-partition-topic";
    val kcm1 = createKafkaCheckpointManager(checkpointTopic)
    kcm1.register(taskName)
    // create topic with the wrong number of partitions
    createTopic(checkpointTopic, 8, new KafkaConfig(config).getCheckpointTopicProperties())
    try {
      kcm1.start
      fail("Expected an exception for invalid number of partitions in the checkpoint topic.")
    } catch {
      case e: StreamValidationException => None
    }
    kcm1.stop

    // Should not fail if failOnTopicValidation = false
    val failOnTopicValidation = false
    val kcm2 = createKafkaCheckpointManager(checkpointTopic, new CheckpointSerde, failOnTopicValidation)
    kcm2.register(taskName)
    try {
      kcm2.start
    } catch {
      case e: KafkaUtilException => fail("Unexpected exception for invalid number of partitions in the checkpoint topic")
    }
    kcm2.stop
  }

  @After
  override def tearDown() {
    if (servers != null) {
      servers.foreach(_.shutdown())
      servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    }
    super.tearDown
  }

  private def getCheckpointProducerProperties() : Properties = {
    val defaultSerializer = classOf[ByteArraySerializer].getCanonicalName
    val props = new Properties()
    props.putAll(ImmutableMap.of(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, defaultSerializer,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, defaultSerializer))
    props
  }

  private def getConfig() : Config = {
    new MapConfig(new ImmutableMap.Builder[String, String]()
      .put(JobConfig.JOB_NAME, "some-job-name")
      .put(JobConfig.JOB_ID, "i001")
      .put(s"systems.$checkpointSystemName.samza.factory", classOf[KafkaSystemFactory].getCanonicalName)
      .put(s"systems.$checkpointSystemName.producer.bootstrap.servers", brokerList)
      .put(s"systems.$checkpointSystemName.consumer.zookeeper.connect", zkConnect)
      .put("task.checkpoint.system", checkpointSystemName)
      .build())
  }

  private def createKafkaCheckpointManager(cpTopic: String, serde: CheckpointSerde = new CheckpointSerde, failOnTopicValidation: Boolean = true) = {
    val kafkaConfig = new org.apache.samza.config.KafkaConfig(config)
    val props = kafkaConfig.getCheckpointTopicProperties()
    val systemName = kafkaConfig.getCheckpointSystem.getOrElse(
      throw new SamzaException("No system defined for Kafka's checkpoint manager."))

    val systemFactoryClassName = new SystemConfig(config)
      .getSystemFactory(systemName)
      .getOrElse(throw new SamzaException("Missing configuration: " + SystemConfig.SYSTEM_FACTORY format systemName))

    val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)

    val spec = new KafkaStreamSpec("id", cpTopic, checkpointSystemName, 1, 1, props)
    new KafkaCheckpointManager(spec, systemFactory, failOnTopicValidation, config, new NoOpMetricsRegistry, serde)
  }

  private def readCheckpoint(checkpointTopic: String, taskName: TaskName) : Checkpoint = {
    val kcm = createKafkaCheckpointManager(checkpointTopic)
    kcm.register(taskName)
    kcm.start
    val checkpoint = kcm.readLastCheckpoint(taskName)
    kcm.stop
    checkpoint
  }

  private def writeCheckpoint(checkpointTopic: String, taskName: TaskName, checkpoint: Checkpoint) = {
    val kcm = createKafkaCheckpointManager(checkpointTopic)
    kcm.register(taskName)
    kcm.start
    kcm.writeCheckpoint(taskName, checkpoint)
  }

  private def createTopic(cpTopic: String, partNum: Int, props: Properties) = {
    val zkClient = ZkUtils(zkConnect, 6000, 6000, JaasUtils.isZkSecurityEnabled())
    try {
      AdminUtils.createTopic(
        zkClient,
        cpTopic,
        partNum,
        1,
        props)
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      zkClient.close
    }
  }

}
