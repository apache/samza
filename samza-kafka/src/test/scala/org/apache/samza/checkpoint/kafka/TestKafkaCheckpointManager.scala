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

import _root_.kafka.admin.AdminUtils
import _root_.kafka.common.{InvalidMessageSizeException, UnknownTopicOrPartitionException}
import _root_.kafka.message.InvalidMessageException
import _root_.kafka.server.{KafkaConfig, KafkaServer, ConfigType}
import _root_.kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import _root_.kafka.integration.KafkaServerTestHarness

import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.system._
import org.apache.samza.util.{NoOpMetricsRegistry, ClientUtilTopicMetadataStore, KafkaUtilException, TopicMetadataStore}
import org.apache.samza.{Partition, SamzaException}
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConverters._
import scala.collection._

class TestKafkaCheckpointManager extends KafkaServerTestHarness {

  protected def numBrokers: Int = 3

  def generateConfigs() = {
    val props = TestUtils.createBrokerConfigs(numBrokers, zkConnect, true)
    props.map(KafkaConfig.fromProps)
  }

  val checkpointTopic = "checkpoint-topic"
  val serdeCheckpointTopic = "checkpoint-topic-invalid-serde"
  val checkpointTopicConfig = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(null)

  val zkSecure = JaasUtils.isZkSecurityEnabled()

  val partition = new Partition(0)
  val partition2 = new Partition(1)
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "123").asJava)
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "12345").asJava)

  var producerConfig: KafkaProducerConfig = null

  var metadataStore: TopicMetadataStore = null
  var failOnTopicValidation = true

  val systemStreamPartitionGrouperFactoryString = classOf[GroupByPartitionFactory].getCanonicalName

  var systemConsumerF: ()=>SystemConsumer = ()=>{null}
  var systemProducerF: ()=>SystemProducer = ()=>{null}
  var systemAdminF: ()=>SystemAdmin = ()=>{null}

  @Before
  override def setUp {
    super.setUp

    TestUtils.waitUntilTrue(() => servers.head.metadataCache.getAliveBrokers.size == numBrokers, "Wait for cache to update")

    val systemName = "kafka"
    val brokers = brokerList.split(",").map(p => "localhost" + p).mkString(",")
    val config = new java.util.HashMap[String, String]()
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    config.put("acks", "all")
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    config.put(ProducerConfig.RETRIES_CONFIG, (new Integer(java.lang.Integer.MAX_VALUE-1)).toString)
    config.putAll(KafkaCheckpointManagerFactory.INJECTED_PRODUCER_PROPERTIES.asJava)
    producerConfig = new KafkaProducerConfig(systemName, "i001", config)

    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")

    config.put(SystemConfig.SYSTEM_FACTORY format systemName, "org.apache.samza.system.kafka.KafkaSystemFactory")
    config.put(org.apache.samza.config.KafkaConfig.CHECKPOINT_SYSTEM, systemName);
    config.put(JobConfig.JOB_NAME, "some-job-name");
    config.put(JobConfig.JOB_ID, "i001");
    config.put("systems.%s.producer.%s" format (systemName, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), brokers)
    config.put("systems.%s.consumer.zookeeper.connect" format systemName, zkConnect)
    val cfg: SystemConfig = new SystemConfig(new MapConfig(config))
    val (sName: String, systemConsumerFactory : SystemFactory) =  org.apache.samza.util.Util.getCheckpointSystemStreamAndFactory(cfg)
    systemConsumerF = () => {systemConsumerFactory.getConsumer(sName, cfg, new NoOpMetricsRegistry())}
    systemProducerF = () => {systemConsumerFactory.getProducer(sName, cfg, new NoOpMetricsRegistry())}
    systemAdminF = () => {systemConsumerFactory.getAdmin(sName, cfg)}
  }

  @After
  override def tearDown() {
    if (servers != null) {
      servers.foreach(_.shutdown())
      servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    }
    super.tearDown
  }

  private def writeCheckpoint(taskName: TaskName, checkpoint: Checkpoint, cpTopic: String = checkpointTopic) = {
    val producer: Producer[Array[Byte], Array[Byte]] = new KafkaProducer(producerConfig.getProducerProperties)
    val record = new ProducerRecord(
      cpTopic,
      0,
      KafkaCheckpointLogKey.getCheckpointKey(taskName).toBytes(),
      new CheckpointSerde().toBytes(checkpoint)
    )
    try {
      producer.send(record).get()
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      producer.close()
    }
  }


  private def createCheckpointTopic(cpTopic: String = checkpointTopic, partNum: Int = 1) = {
    val zkClient = ZkUtils(zkConnect, 6000, 6000, zkSecure)
    try {
      AdminUtils.createTopic(
        zkClient,
        cpTopic,
        partNum,
        1,
        checkpointTopicConfig)
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      zkClient.close
    }
  }

  @Test
  def testCheckpointShouldBeNullIfCheckpointTopicDoesNotExistShouldBeCreatedOnWriteAndShouldBeReadableAfterWrite {
    val kcm = getKafkaCheckpointManager
    val taskName = new TaskName(partition.toString)
    kcm.register(taskName)
    createCheckpointTopic()
    kcm.kafkaUtil.validateTopicPartitionCount(checkpointTopic, "kafka", metadataStore, 1)

    // check that log compaction is enabled.
    val zkClient = ZkUtils(zkConnect, 6000, 6000, zkSecure)
    val topicConfig = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, checkpointTopic)
    zkClient.close
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))

    // read before topic exists should result in a null checkpoint
    var readCp = kcm.readLastCheckpoint(taskName)
    assertNull(readCp)

    // create topic the first time around
    writeCheckpoint(taskName, cp1)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp1, readCp)

    // should get an exception if partition doesn't exist
    try {
      readCp = kcm.readLastCheckpoint(new TaskName(new Partition(1).toString))
      fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    } catch {
      case e: SamzaException => None // expected
      case _: Exception => fail("Expected a SamzaException, since only one partition (partition 0) should exist.")
    }

    // writing a second message should work, too
    writeCheckpoint(taskName, cp2)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp2, readCp)
    kcm.stop
  }


  @Test
  def testCheckpointReadTwice {
    val kcm = getKafkaCheckpointManager
    val taskName = new TaskName(partition.toString)
    kcm.register(taskName)
    createCheckpointTopic()
    kcm.kafkaUtil.validateTopicPartitionCount(checkpointTopic, "kafka", metadataStore, 1)

    // check that log compaction is enabled.
    val zkClient = ZkUtils(zkConnect, 6000, 6000, zkSecure)
    val topicConfig = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, checkpointTopic)
    zkClient.close
    assertEquals("compact", topicConfig.get("cleanup.policy"))
    assertEquals("26214400", topicConfig.get("segment.bytes"))

    // read before topic exists should result in a null checkpoint
    var readCp = kcm.readLastCheckpoint(taskName)
    assertNull(readCp)

    // create topic the first time around
    writeCheckpoint(taskName, cp1)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp1, readCp)

    // writing a second message should work, too
    writeCheckpoint(taskName, cp2)
    readCp = kcm.readLastCheckpoint(taskName)
    assertEquals(cp2, readCp)
    kcm.stop

    // get new KCM for the same stream
    val kcm1 = getKafkaCheckpointManager
    kcm1.register(taskName)
    readCp = kcm1.readLastCheckpoint(taskName)
    assertEquals(cp2, readCp)
    kcm1.stop
  }

  @Test
  def testUnrecoverableKafkaErrorShouldThrowKafkaCheckpointManagerException {
    val exceptions = List("InvalidMessageException", "InvalidMessageSizeException", "UnknownTopicOrPartitionException")
    exceptions.foreach { exceptionName =>
      val kcm = getKafkaCheckpointManagerWithInvalidSerde(exceptionName)
      val taskName = new TaskName(partition.toString)
      kcm.register(taskName)
      createCheckpointTopic(serdeCheckpointTopic)
      kcm.kafkaUtil.validateTopicPartitionCount(serdeCheckpointTopic, "kafka", metadataStore, 1)
      writeCheckpoint(taskName, cp1, serdeCheckpointTopic)
      // because serde will throw unrecoverable errors, it should result a KafkaCheckpointException
      try {
        kcm.readLastCheckpoint(taskName)
        fail("Expected an Exception.")
      } catch {
        case e: KafkaUtilException => None
        case e: Exception => None
      }
      kcm.stop
    }
  }

  @Test
  def testFailOnTopicValidation {
    // first case - default case, we should fail on validation
    failOnTopicValidation = true
    val checkpointTopic8 = checkpointTopic + "8";
    val kcm = getKafkaCheckpointManagerWithParam(checkpointTopic8)
    val taskName = new TaskName(partition.toString)
    kcm.register(taskName)
    createCheckpointTopic(checkpointTopic8, 8) // create topic with the wrong number of partitions
    try {
      kcm.start
      fail("Expected a KafkaUtilException for invalid number of partitions in the topic.")
    }catch {
      case e: KafkaUtilException => None
    }
    kcm.stop

    // same validation but ignore the validation error (pass 'false' to validate..)
    failOnTopicValidation = false
    val kcm1 = getKafkaCheckpointManagerWithParam((checkpointTopic8))
    kcm1.register(taskName)
    try {
      kcm1.start
    }catch {
      case e: KafkaUtilException => fail("Did not expect a KafkaUtilException for invalid number of partitions in the topic.")
    }
    kcm1.stop
  }

  private def getKafkaCheckpointManagerWithParam(cpTopic: String) = new KafkaCheckpointManager(
    clientId = "some-client-id",
    checkpointTopic = cpTopic,
    systemName = "kafka",
    replicationFactor = 3,
    socketTimeout = 30000,
    bufferSize = 64 * 1024,
    fetchSize = 300 * 1024,
    getSystemConsumer = systemConsumerF,
    getSystemAdmin = systemAdminF,
    metadataStore = metadataStore,
    getSystemProducer = systemProducerF,
    connectZk = () => ZkUtils(zkConnect, 6000, 6000, zkSecure),
    systemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString,
    failOnCheckpointValidation = failOnTopicValidation,
    checkpointTopicProperties = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(new MapConfig(Map[String, String]().asJava)))

  // CheckpointManager with a specific checkpoint topic
  private def getKafkaCheckpointManager = getKafkaCheckpointManagerWithParam(checkpointTopic)

  // inject serde. Kafka exceptions will be thrown when serde.fromBytes is called
  private def getKafkaCheckpointManagerWithInvalidSerde(exception: String) = new KafkaCheckpointManager(
    clientId = "some-client-id-invalid-serde",
    checkpointTopic = serdeCheckpointTopic,
    systemName = "kafka",
    replicationFactor = 3,
    socketTimeout = 30000,
    bufferSize = 64 * 1024,
    fetchSize = 300 * 1024,
    getSystemConsumer = systemConsumerF,
    getSystemAdmin = systemAdminF,
    metadataStore = metadataStore,
    getSystemProducer = systemProducerF,
    connectZk = () => ZkUtils(zkConnect, 6000, 6000, zkSecure),
    systemStreamPartitionGrouperFactoryString = systemStreamPartitionGrouperFactoryString,
    failOnCheckpointValidation = failOnTopicValidation,
    serde = new InvalideSerde(exception),
    checkpointTopicProperties = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(new MapConfig(Map[String, String]().asJava)))

  class InvalideSerde(exception: String) extends CheckpointSerde {
    override def fromBytes(bytes: Array[Byte]): Checkpoint = {
      exception match {
        case "InvalidMessageException" => throw new InvalidMessageException
        case "InvalidMessageSizeException" => throw new InvalidMessageSizeException
        case "UnknownTopicOrPartitionException" => throw new UnknownTopicOrPartitionException
      }
    }
  }
}
