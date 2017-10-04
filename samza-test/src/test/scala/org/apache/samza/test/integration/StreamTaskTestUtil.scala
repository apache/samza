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

import java.util
import java.util.Properties
import java.util.concurrent.{CountDownLatch, TimeUnit}
import javax.security.auth.login.Configuration

import kafka.admin.AdminUtils
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.message.MessageAndMetadata
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, CoreUtils, ZkUtils}
import kafka.zk.EmbeddedZookeeper
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.samza.Partition
import org.apache.samza.checkpoint.Checkpoint
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.JaasUtils
import org.apache.samza.config.{ApplicationConfig, Config, KafkaProducerConfig, MapConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.job.local.ThreadJobFactory
import org.apache.samza.job.{ApplicationStatus, JobRunner, StreamJob}
import org.apache.samza.system.kafka.TopicMetadataCache
import org.apache.samza.system.{IncomingMessageEnvelope, SystemStreamPartition}
import org.apache.samza.task._
import org.apache.samza.util.{ClientUtilTopicMetadataStore, KafkaUtil, TopicMetadataStore}
import org.junit.Assert._

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap, SynchronizedMap}

/*
 * This creates an singleton instance of TestBaseStreamTask and implement the helper functions to
 * 1. start the local ZooKeeper server
 * 2. start the local Kafka brokers
 * 3. create and validate test topics
 * 4. shutdown servers and cleanup test directories and files
 */
object StreamTaskTestUtil {
  val INPUT_TOPIC = "input"
  val TOTAL_TASK_NAMES = 1
  val REPLICATION_FACTOR = 3

  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  var zkUtils: ZkUtils = null
  var zookeeper: EmbeddedZookeeper = null
  var brokers: String = null
  def zkPort: Int = zookeeper.port
  def zkConnect: String = s"127.0.0.1:$zkPort"

  var producer: Producer[Array[Byte], Array[Byte]] = null
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", new Partition(0)) -> "123").asJava)
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", new Partition(0)) -> "12345").asJava)

  var metadataStore: TopicMetadataStore = null

  /*
   * This is the default job configuration. Each test class can override the default configuration below.
   */
  var jobConfig = Map(
    "job.factory.class" -> classOf[ThreadJobFactory].getCanonicalName,
    "job.coordinator.system" -> "kafka",
    ApplicationConfig.PROCESSOR_ID -> "1",
    "task.inputs" -> "kafka.input",
    "serializers.registry.string.class" -> "org.apache.samza.serializers.StringSerdeFactory",
    "systems.kafka.samza.factory" -> "org.apache.samza.system.kafka.KafkaSystemFactory",
    // Always start consuming at offset 0. This avoids a race condition between
    // the producer and the consumer in this test (SAMZA-166, SAMZA-224).
    "systems.kafka.samza.offset.default" -> "oldest", // applies to a nonempty topic
    "systems.kafka.consumer.auto.offset.reset" -> "smallest", // applies to an empty topic
    "systems.kafka.samza.msg.serde" -> "string",
    // Since using state, need a checkpoint manager
    "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
    "task.checkpoint.system" -> "kafka",
    "task.checkpoint.replication.factor" -> "1",
    // However, don't have the inputs use the checkpoint manager
    // since the second part of the test expects to replay the input streams.
    "systems.kafka.streams.input.samza.reset.offset" -> "false")

  def apply(map: Map[String, String]): Unit = {
    jobConfig ++= map
    TestTask.reset()
  }

  var servers: Buffer[KafkaServer] = null

  def beforeSetupServers {
    zookeeper = new EmbeddedZookeeper()
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, JaasUtils.isZkSecurityEnabled())

    val props = TestUtils.createBrokerConfigs(3, zkConnect, true)

    val configs = props.map(p => {
      p.setProperty("auto.create.topics.enable","false")
      KafkaConfig.fromProps(p)
    })

    servers = configs.map(TestUtils.createServer(_)).toBuffer

    val brokerList = TestUtils.getBrokerListStrFromServers(servers, SecurityProtocol.PLAINTEXT)
    brokers = brokerList.split(",").map(p => "127.0.0.1" + p).mkString(",")

    // setup the zookeeper and bootstrap servers for local kafka cluster
    jobConfig ++= Map("systems.kafka.consumer.zookeeper.connect" -> zkConnect,
      "systems.kafka.producer.bootstrap.servers" -> brokers)

    val config = new util.HashMap[String, String]()

    config.put("bootstrap.servers", brokers)
    config.put("request.required.acks", "-1")
    config.put("serializer.class", "kafka.serializer.StringEncoder")
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    config.put(ProducerConfig.RETRIES_CONFIG, (new Integer(Integer.MAX_VALUE-1)).toString())
    config.put(ProducerConfig.LINGER_MS_CONFIG, "0")
    val producerConfig = new KafkaProducerConfig("kafka", "i001", config)

    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")

    createTopics
    validateTopics
  }

  def createTopics {
    AdminUtils.createTopic(
      zkUtils,
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

          KafkaUtil.maybeThrowException(errorCode)
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

  def afterCleanLogDirs {
    servers.foreach(_.shutdown())
    servers.foreach(server => CoreUtils.delete(server.config.logDirs))

    if (zkUtils != null)
     CoreUtils.swallow(zkUtils.close())
    if (zookeeper != null)
      CoreUtils.swallow(zookeeper.shutdown())
    Configuration.setConfiguration(null)

  }
}

/* This class implement the base utility to implement an integration test for StreamTask
 * It implements helper functions to start/stop the job, send messages to a task, and read all messages from a topic
 */
class StreamTaskTestUtil {
  import StreamTaskTestUtil._

  /**
   * Start a job for TestTask, and do some basic sanity checks around startup
   * time, number of partitions, etc.
   */
  def startJob = {
    // Start task.
    val job = new JobRunner(new MapConfig(jobConfig.asJava)).run()
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
    val status = job.waitForFinish(60000)
    assertEquals(ApplicationStatus.UnsuccessfulFinish, status)
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
    val stream = consumerConnector.createMessageStreams(Map(topic -> 1))(topic).head.iterator
    var message: MessageAndMetadata[Array[Byte], Array[Byte]] = null
    var messages = ArrayBuffer[String]()

    while (message == null || message.offset < maxOffsetInclusive) {
      message = stream.next
      if (message.message == null) {
        messages += null
      } else {
        messages += new String(message.message, "UTF-8")
      }
      System.err.println("StreamTaskTestUtil.readAll(): offset=%s, message=%s" format (message.offset, messages.last))
    }

    consumerConnector.shutdown

    messages.toList
  }

}

object TestTask {
  val tasks = new HashMap[TaskName, TestTask] with SynchronizedMap[TaskName, TestTask]
  var totalTasks = 1
  @volatile var allTasksRegistered = new CountDownLatch(totalTasks)

  def reset(): Unit = {
    TestTask.totalTasks = StreamTaskTestUtil.TOTAL_TASK_NAMES
    TestTask.allTasksRegistered = new CountDownLatch(TestTask.totalTasks)
  }

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
    assertEquals(totalTasks, tasks.size)
    // Reset the registered latch, so we can use it again every time we start a new job.
    TestTask.allTasksRegistered = new CountDownLatch(TestTask.totalTasks)
  }
}

/**
 * This class defines the base class for StreamTask used in integration test
 * It implements some basic hooks for synchronization between the test class and the tasks
 */
abstract class TestTask extends StreamTask with InitableTask {
  var received = ArrayBuffer[String]()
  val initFinished = new CountDownLatch(1)
  var gotMessage = new CountDownLatch(1)

  def init(config: Config, context: TaskContext) {
    TestTask.register(context.getTaskName, this)
    testInit(config, context)
    initFinished.countDown()
  }

  def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    val msg = envelope.getMessage.asInstanceOf[String]

    System.err.println("TestTask.process(): %s" format msg)

    received += msg

    testProcess(envelope, collector, coordinator)

    // Notify sender that we got a message.
    gotMessage.countDown
  }

  def awaitMessage {
    assertTrue("Timed out of waiting for message rather than received one.", gotMessage.await(60, TimeUnit.SECONDS))
    assertEquals(0, gotMessage.getCount)
    gotMessage = new CountDownLatch(1)
  }

  def testInit(config: Config, context: TaskContext)

  def testProcess(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator)

}
