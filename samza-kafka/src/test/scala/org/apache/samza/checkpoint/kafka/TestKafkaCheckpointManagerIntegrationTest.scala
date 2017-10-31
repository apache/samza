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
import kafka.common.{InvalidMessageSizeException, UnknownTopicOrPartitionException}
import kafka.integration.KafkaServerTestHarness
import kafka.message.InvalidMessageException
import kafka.server.{ConfigType, KafkaConfig}
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import com.google.common.collect.ImmutableMap
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system._
import org.apache.samza.system.kafka.{KafkaStreamSpec, KafkaSystemFactory}
import org.apache.samza.util.{KafkaUtilException, NoOpMetricsRegistry}
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConverters._
import scala.collection._

class TestKafkaCheckpointManagerIntegrationTest extends KafkaServerTestHarness {

  protected def numBrokers: Int = 3

  val checkpointSystemName = "kafka"
  val sspGrouperFactoryName = classOf[GroupByPartitionFactory].getCanonicalName

  val ssp = new SystemStreamPartition("kafka", "topic", new Partition(0))
  val checkpoint1 = new Checkpoint(Map(ssp -> "offset-1").asJava)
  val checkpoint2 = new Checkpoint(Map(ssp -> "offset-2").asJava)
  val taskName = new TaskName("Partition 0")

  var brokers: String = null
  var config: Config = null

  @Before
  override def setUp {
    super.setUp
    TestUtils.waitUntilTrue(() => servers.head.metadataCache.getAliveBrokers.size == numBrokers, "Wait for cache to update")
    brokers = brokerList.split(",").map(p => "localhost" + p).mkString(",")
    config = getConfig()
  }

  override def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, true)
    props.map(KafkaConfig.fromProps)
  }

  @Test
  def testCheckpointShouldBeNullIfCheckpointTopicDoesNotExistShouldBeCreatedOnWriteAndShouldBeReadableAfterWrite {
    val checkpointTopic = "checkpoint-topic-1"
    val kcm = createKafkaCheckpointManager(checkpointTopic)
    kcm.register(taskName)
    kcm.start

    // check that log compaction is enabled.
    val zkClient = ZkUtils(zkConnect, 6000, 6000, JaasUtils.isZkSecurityEnabled())
    val topicConfig = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, checkpointTopic)

    assertEquals(topicConfig, KafkaCheckpointManagerFactory.getCheckpointTopicProperties(config))
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))

    zkClient.close

    // read before topic exists should result in a null checkpoint
    var readCp = kcm.readLastCheckpoint(taskName)
    assertNull(readCp)

    kcm.writeCheckpoint(taskName, checkpoint1)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(checkpoint1, readCp)

    // should get an exception if partition doesn't exist
    try {
      readCp = kcm.readLastCheckpoint(new TaskName(new Partition(1).toString))
      fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    } catch {
      case e: SamzaException => None // expected
      case _: Exception => fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    }

    // writing a second message should work, too
    kcm.writeCheckpoint(taskName, checkpoint2)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(checkpoint2, readCp)
    kcm.stop
  }

  @Test
  def testFailOnTopicValidation {
    // first case - default case, we should fail on validation
    val checkpointTopic = "eight-partition-topic";
    val kcm = createKafkaCheckpointManager(checkpointTopic)
    kcm.register(taskName)
    createTopic(checkpointTopic, 8, KafkaCheckpointManagerFactory.getCheckpointTopicProperties(config)) // create topic with the wrong number of partitions
    try {
      kcm.start
      fail("Expected a KafkaUtilException for invalid number of partitions in the topic.")
    }catch {
      case e: StreamValidationException => None
    }
    kcm.stop

    // same validation but ignore the validation error (pass 'false' to validate..)
    val failOnTopicValidation = false
    val kcm1 = createKafkaCheckpointManager(checkpointTopic, new CheckpointSerde, failOnTopicValidation)
    kcm1.register(taskName)
    try {
      kcm1.start
    }catch {
      case e: KafkaUtilException => fail("Did not expect a KafkaUtilException for invalid number of partitions in the topic.")
    }
    kcm1.stop
  }

  @After
  override def tearDown() {
    if (servers != null) {
      servers.foreach(_.shutdown())
      servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    }
    println("delete data!")
    super.tearDown
  }

  private def getCheckpointProducerProperties() : Properties = {
    val defaultSerializer = classOf[ByteArraySerializer].getCanonicalName
    val props = new Properties()
    props.putAll(ImmutableMap.of(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, defaultSerializer,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, defaultSerializer))
    props
  }

  private def getConfig() : Config = {
    new MapConfig(new ImmutableMap.Builder[String, String]()
      .put(JobConfig.JOB_NAME, "some-job-name")
      .put(JobConfig.JOB_ID, "i001")
      .put(JobConfig.SSP_GROUPER_FACTORY, sspGrouperFactoryName)
      .put(s"systems.$checkpointSystemName.samza.factory", classOf[KafkaSystemFactory].getCanonicalName)
      .put(s"systems.$checkpointSystemName.producer.bootstrap.servers", brokers)
      .put(s"systems.$checkpointSystemName.consumer.zookeeper.connect", zkConnect)
      .put("task.checkpoint.system", checkpointSystemName)
      .build())
  }

  private def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint, topic: String) = {
    val producer: Producer[Array[Byte], Array[Byte]] = new KafkaProducer(getCheckpointProducerProperties())
    val checkpointKeySerde = new KafkaCheckpointLogKeySerde

    val checkpointKeyBytes = checkpointKeySerde.toBytes(new KafkaCheckpointLogKey(sspGrouperFactoryName, taskName, KafkaCheckpointLogKey.CHECKPOINT_TYPE))
    val checkpointMsgBytes = new CheckpointSerde().toBytes(checkpoint)
    val record = new ProducerRecord(topic, 0, checkpointKeyBytes, checkpointMsgBytes)
    try {
      producer.send(record).get()
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      producer.close()
    }
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

  private def createKafkaCheckpointManager(cpTopic: String, serde: CheckpointSerde = new CheckpointSerde, failOnTopicValidation: Boolean = true) = {
    val props = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(config)
    val (checkpointSystem: String, factory : SystemFactory) =
      KafkaCheckpointManagerFactory.getCheckpointSystemStreamAndFactory(config)

    println(props)

    val spec = new KafkaStreamSpec("id", cpTopic, checkpointSystemName, 1, 1, props)
    new KafkaCheckpointManager(spec, factory, failOnTopicValidation, config, new NoOpMetricsRegistry, serde)
  }
}
