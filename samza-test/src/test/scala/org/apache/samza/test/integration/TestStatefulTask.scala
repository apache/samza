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

package org.apache.samza.test.integration

import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import kafka.admin.AdminUtils
import kafka.common.ErrorMapping
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.message.MessageAndMetadata
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.utils.TestZKUtils
import kafka.utils.Utils
import kafka.utils.ZKStringSerializer
import kafka.zk.EmbeddedZookeeper

import org.I0Itec.zkclient.ZkClient
import org.apache.samza.Partition
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.config.Config
import org.apache.samza.job.local.ThreadJobFactory
import org.apache.samza.config.MapConfig
import org.apache.samza.container.TaskName
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.StreamJob
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.system.kafka.TopicMetadataCache
import org.apache.samza.system.{SystemStreamPartition, IncomingMessageEnvelope}
import org.apache.samza.config.KafkaProducerConfig
import org.apache.samza.task.InitableTask
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskContext
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.TaskCoordinator.RequestScope
import org.apache.samza.util.ClientUtilTopicMetadataStore
import org.apache.samza.util.TopicMetadataStore
import org.junit.Assert._
import org.junit.{BeforeClass, AfterClass, Test}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedMap
import org.apache.kafka.clients.producer.{ProducerConfig, Producer, ProducerRecord, KafkaProducer}
import java.util


object TestStatefulTask {
  val INPUT_TOPIC = "input"
  val STORE_NAME = "mystore"
  val STATE_TOPIC_STREAM = "mystoreChangelog"
  val TOTAL_TASK_NAMES = 1
  val REPLICATION_FACTOR = 3

  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  val brokerId1 = 0
  val brokerId2 = 1
  val brokerId3 = 2
  val ports = TestUtils.choosePorts(3)
  val (port1, port2, port3) = (ports(0), ports(1), ports(2))

  val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
  val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  val props3 = TestUtils.createBrokerConfig(brokerId3, port3)
  props1.setProperty("auto.create.topics.enable","false")
  props2.setProperty("auto.create.topics.enable","false")
  props3.setProperty("auto.create.topics.enable","false")

  val config = new util.HashMap[String, Object]()
  val brokers = "localhost:%d,localhost:%d,localhost:%d" format (port1, port2, port3)
  config.put("bootstrap.servers", brokers)
  config.put("request.required.acks", "-1")
  config.put("serializer.class", "kafka.serializer.StringEncoder")
  config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, new Integer(1))
  config.put(ProducerConfig.RETRIES_CONFIG, new Integer(Integer.MAX_VALUE-1))
  val producerConfig = new KafkaProducerConfig("kafka", "i001", config)
  var producer: Producer[Array[Byte], Array[Byte]] = null
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", new Partition(0)) -> "123"))
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", new Partition(0)) -> "12345"))
  var zookeeper: EmbeddedZookeeper = null
  var server1: KafkaServer = null
  var server2: KafkaServer = null
  var server3: KafkaServer = null
  var metadataStore: TopicMetadataStore = null

  @BeforeClass
  def beforeSetupServers {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    server1 = TestUtils.createServer(new KafkaConfig(props1))
    server2 = TestUtils.createServer(new KafkaConfig(props2))
    server3 = TestUtils.createServer(new KafkaConfig(props3))
    zkClient = new ZkClient(zkConnect + "/", 6000, 6000, ZKStringSerializer)
    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")

    createTopics
    validateTopics
  }

  def createTopics {
    AdminUtils.createTopic(
      zkClient,
      INPUT_TOPIC,
      TOTAL_TASK_NAMES,
      REPLICATION_FACTOR)
  }

  def validateTopics {
    val topics = Set(INPUT_TOPIC)
    var done = false
    var retries = 0

    while (!done && retries < 100) {
      try {
        val topicMetadataMap = TopicMetadataCache.getTopicMetadata(topics, "kafka", metadataStore.getTopicInfo)

        topics.foreach(topic => {
          val topicMetadata = topicMetadataMap(topic)
          val errorCode = topicMetadata.errorCode

          ErrorMapping.maybeThrowException(errorCode)
        })

        done = true
      } catch {
        case e: Exception =>
          System.err.println("Got exception while validating test topics. Waiting and retrying.", e)
          retries += 1
          Thread.sleep(500)
      }
    }

    if (retries >= 100) {
      fail("Unable to successfully create topics. Tried to validate %s times." format retries)
    }
  }

  @AfterClass
  def afterCleanLogDirs {
    producer.close()
    server1.shutdown
    server1.awaitShutdown()
    server2.shutdown
    server2.awaitShutdown()
    server3.shutdown
    server3.awaitShutdown()
    Utils.rm(server1.config.logDirs)
    Utils.rm(server2.config.logDirs)
    Utils.rm(server3.config.logDirs)
    zkClient.close
    zookeeper.shutdown
  }
}

/**
 * Test that does the following:
 *
 * 1. Starts ZK, and 3 kafka brokers.
 * 2. Create two topics: input and mystore.
 * 3. Validate that the topics were created successfully and have leaders.
 * 4. Start a single partition of TestTask using ThreadJobFactory.
 * 5. Send four messages to input (1,2,3,2), which contain one dupe (2).
 * 6. Validate that all messages were received by TestTask.
 * 7. Validate that TestTask called store.put() for all four messages, and that the messages ended up in the mystore topic.
 * 8. Kill the job.
 * 9. Start the job again.
 * 10. Validate that the job restored all messages (1,2,3) to the store.
 * 11. Send three more messages to input (4,5,5), and validate that TestTask receives them.
 * 12. Kill the job again.
 */
class TestStatefulTask {
  import TestStatefulTask._
  val jobFactory = new ThreadJobFactory

  val jobConfig = Map(
    "job.factory.class" -> jobFactory.getClass.getCanonicalName,
    "job.name" -> "hello-stateful-world",
    "task.class" -> "org.apache.samza.test.integration.TestTask",
    "task.inputs" -> "kafka.input",
    "serializers.registry.string.class" -> "org.apache.samza.serializers.StringSerdeFactory",
    "stores.mystore.factory" -> "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory",
    "stores.mystore.key.serde" -> "string",
    "stores.mystore.msg.serde" -> "string",
    "stores.mystore.changelog" -> "kafka.mystoreChangelog",
    "stores.mystore.changelog.replication.factor" -> "1",

    "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
    // Always start consuming at offset 0. This avoids a race condition between
    // the producer and the consumer in this test (SAMZA-166, SAMZA-224).
    "systems.kafka.samza.offset.default" -> "oldest", // applies to a nonempty topic
    "systems.kafka.consumer.auto.offset.reset" -> "smallest", // applies to an empty topic
    "systems.kafka.samza.msg.serde" -> "string",
    "systems.kafka.consumer.zookeeper.connect" -> zkConnect,
    "systems.kafka.producer.bootstrap.servers" -> ("localhost:%s" format port1),
    // Since using state, need a checkpoint manager
    "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
    "task.checkpoint.system" -> "kafka",
    "task.checkpoint.replication.factor" -> "1",
    // However, don't have the inputs use the checkpoint manager
    // since the second part of the test expects to replay the input streams.
    "systems.kafka.streams.input.samza.reset.offset" -> "true")

  @Test
  def testShouldStartAndRestore {
    // Have to do this in one test to guarantee ordering.
    testShouldStartTaskForFirstTime
    testShouldRestoreStore
  }

  def testShouldStartTaskForFirstTime {
    val (job, task) = startJob

    // Validate that restored is empty.
    assertEquals(0, task.initFinished.getCount)
    assertEquals(0, task.restored.size)
    assertEquals(0, task.received.size)

    // Send some messages to input stream.
    send(task, "1")
    send(task, "2")
    send(task, "3")
    send(task, "2")
    send(task, "99")
    send(task, "-99")

    // Validate that messages appear in store stream.
    val messages = readAll(STATE_TOPIC_STREAM, 5, "testShouldStartTaskForFirstTime")

    assertEquals(6, messages.length)
    assertEquals("1", messages(0))
    assertEquals("2", messages(1))
    assertEquals("3", messages(2))
    assertEquals("2", messages(3))
    assertEquals("99", messages(4))
    assertNull(messages(5))

    stopJob(job)
  }

  def testShouldRestoreStore {
    val (job, task) = startJob

    // Validate that restored has expected data.
    assertEquals(3, task.restored.size)
    assertTrue(task.restored.contains("1"))
    assertTrue(task.restored.contains("2"))
    assertTrue(task.restored.contains("3"))

    var count = 0

    // We should get the original four messages in the stream (1,2,3,2).
    // Note that this will trigger four new outgoing messages to the STATE_TOPIC.
    while (task.received.size < 4 && count < 100) {
      Thread.sleep(600)
      count += 1
    }

    assertTrue("Timed out waiting to received messages. Received thus far: " + task.received.size, count < 100)

    // Reset the count down latch after the 4 messages come in.
    task.awaitMessage

    // Send some messages to input stream.
    send(task, "4")
    send(task, "5")
    send(task, "5")

    // Validate that messages appear in store stream.
    val messages = readAll(STATE_TOPIC_STREAM, 14, "testShouldRestoreStore")

    assertEquals(15, messages.length)
    // From initial start.
    assertEquals("1", messages(0))
    assertEquals("2", messages(1))
    assertEquals("3", messages(2))
    assertEquals("2", messages(3))
    assertEquals("99", messages(4))
    assertNull(messages(5))
    // From second startup.
    assertEquals("1", messages(6))
    assertEquals("2", messages(7))
    assertEquals("3", messages(8))
    assertEquals("2", messages(9))
    assertEquals("99", messages(10))
    assertNull(messages(11))
    // From sending in this method.
    assertEquals("4", messages(12))
    assertEquals("5", messages(13))
    assertEquals("5", messages(14))

    stopJob(job)
  }

  /**
   * Start a job for TestJob, and do some basic sanity checks around startup
   * time, number of partitions, etc.
   */
  def startJob = {
    val job = jobFactory.getJob(new MapConfig(jobConfig))

    // Start task.
    job.submit
    assertEquals(ApplicationStatus.Running, job.waitForStatus(ApplicationStatus.Running, 60000))
    TestTask.awaitTaskRegistered
    val tasks = TestTask.tasks

    assertEquals("Should only have a single partition in this task", 1, tasks.size)

    val task = tasks.values.toList.head

    task.initFinished.await(60, TimeUnit.SECONDS)
    assertEquals(0, task.initFinished.getCount)

    (job, task)
  }

  /**
   * Kill a job, and wait for an unsuccessful finish (since this throws an
   * interrupt, which is forwarded on to ThreadJob, and marked as a failure).
   */
  def stopJob(job: StreamJob) {
    // Shutdown task.
    job.kill
    assertEquals(ApplicationStatus.UnsuccessfulFinish, job.waitForFinish(60000))
  }

  /**
   * Send a message to the input topic, and validate that it gets to the test task.
   */
  def send(task: TestTask, msg: String) {
    producer.send(new ProducerRecord(INPUT_TOPIC, msg.getBytes)).get()
    task.awaitMessage
    assertEquals(msg, task.received.last)
  }

  /**
   * Read all messages from a topic starting from last saved offset for group.
   * To read all from offset 0, specify a unique, new group string.
   */
  def readAll(topic: String, maxOffsetInclusive: Int, group: String): List[String] = {
    val props = new Properties

    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", group)
    props.put("auto.offset.reset", "smallest")

    val consumerConfig = new ConsumerConfig(props)
    val consumerConnector = Consumer.create(consumerConfig)
    var stream = consumerConnector.createMessageStreams(Map(topic -> 1)).get(topic).get.get(0).iterator
    var message: MessageAndMetadata[Array[Byte], Array[Byte]] = null
    var messages = ArrayBuffer[String]()

    while (message == null || message.offset < maxOffsetInclusive) {
      message = stream.next
      if (message.message == null) {
        messages += null
      } else {
        messages += new String(message.message, "UTF-8")
      }
      System.err.println("TestStatefulTask.readAll(): offset=%s, message=%s" format (message.offset, messages.last))
    }

    consumerConnector.shutdown

    messages.toList
  }
}

object TestTask {
  val tasks = new HashMap[TaskName, TestTask] with SynchronizedMap[TaskName, TestTask]
  @volatile var allTasksRegistered = new CountDownLatch(TestStatefulTask.TOTAL_TASK_NAMES)

  /**
   * Static method that tasks can use to register themselves with. Useful so
   * we don't have to sneak into the ThreadJob/SamzaContainer to get our test
   * tasks.
   */
  def register(taskName: TaskName, task: TestTask) {
    tasks += taskName -> task
    allTasksRegistered.countDown
  }

  def awaitTaskRegistered {
    allTasksRegistered.await(60, TimeUnit.SECONDS)
    assertEquals(0, allTasksRegistered.getCount)
    assertEquals(TestStatefulTask.TOTAL_TASK_NAMES, tasks.size)
    // Reset the registered latch, so we can use it again every time we start a new job.
    TestTask.allTasksRegistered = new CountDownLatch(TestStatefulTask.TOTAL_TASK_NAMES)
  }
}

class TestTask extends StreamTask with InitableTask {
  var store: KeyValueStore[String, String] = null
  var restored = Set[String]()
  var received = ArrayBuffer[String]()
  val initFinished = new CountDownLatch(1)
  var gotMessage = new CountDownLatch(1)

  def init(config: Config, context: TaskContext) {
    TestTask.register(context.getTaskName, this)
    store = context
      .getStore(TestStatefulTask.STORE_NAME)
      .asInstanceOf[KeyValueStore[String, String]]
    val iter = store.all
    restored ++= iter
      .map(_.getValue)
      .toSet
    System.err.println("TestTask.init(): %s" format restored)
    iter.close
    initFinished.countDown()
  }

  def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    val msg = envelope.getMessage.asInstanceOf[String]

    System.err.println("TestTask.process(): %s" format msg)

    received += msg

    // A negative string means delete
    if (msg.startsWith("-")) {
      store.delete(msg.substring(1))
    } else {
      store.put(msg, msg)
    }

    coordinator.commit(RequestScope.ALL_TASKS_IN_CONTAINER)

    // Notify sender that we got a message.
    gotMessage.countDown
  }

  def awaitMessage {
    assertTrue("Timed out of waiting for message rather than received one.", gotMessage.await(60, TimeUnit.SECONDS))
    assertEquals(0, gotMessage.getCount)
    gotMessage = new CountDownLatch(1)
  }
}
