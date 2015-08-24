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

package org.apache.samza.container

import java.util.concurrent.ConcurrentHashMap
import org.apache.samza.SamzaException
import org.apache.samza.Partition
import org.apache.samza.checkpoint.OffsetManager
import org.apache.samza.config.Config
import org.apache.samza.config.MapConfig
import org.apache.samza.metrics.Counter
import org.apache.samza.metrics.Metric
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemConsumers
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.SystemProducers
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.task._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemAdmin
import scala.collection.mutable.ListBuffer

class TestTaskInstance {
  @Test
  def testOffsetsAreUpdatedOnProcess {
    val task = new StreamTask {
      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
      }
    }
    val config = new MapConfig
    val partition = new Partition(0)
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val systemStream = new SystemStream("test-system", "test-stream")
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val systemStreamPartitions = Set(systemStreamPartition)
    // Pretend our last checkpointed (next) offset was 2.
    val testSystemStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val offsetManager = OffsetManager(Map(systemStream -> testSystemStreamMetadata), config)
    val taskName = new TaskName("taskName")
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val containerContext = new SamzaContainerContext(0, config, Set[TaskName](taskName))
    val taskInstance: TaskInstance = new TaskInstance(
      task,
      taskName,
      config,
      new TaskInstanceMetrics,
      null,
      consumerMultiplexer,
      collector,
      containerContext,
      offsetManager,
      systemStreamPartitions = systemStreamPartitions)
    // Pretend we got a message with offset 2 and next offset 3.
    val coordinator = new ReadableCoordinator(taskName)
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "2", null, null), coordinator)
    // Check to see if the offset manager has been properly updated with offset 3.
    val lastProcessedOffset = offsetManager.getLastProcessedOffset(taskName, systemStreamPartition)
    assertTrue(lastProcessedOffset.isDefined)
    assertEquals("2", lastProcessedOffset.get)
  }

  /**
   * Mock exception used to test exception counts metrics.
   */
  class TroublesomeException extends RuntimeException {
  }

  /**
   * Mock exception used to test exception counts metrics.
   */
  class NonFatalException extends RuntimeException {
  }

  /**
   * Mock exception used to test exception counts metrics.
   */
  class FatalException extends RuntimeException {
  }

  /**
   * Task used to test exception counts metrics.
   */
  class TroublesomeTask extends StreamTask with WindowableTask {
    def process(
      envelope: IncomingMessageEnvelope,
      collector: MessageCollector,
      coordinator: TaskCoordinator) {

      envelope.getOffset().toInt match {
        case offset if offset % 2 == 0 => throw new TroublesomeException
        case _ => throw new NonFatalException
      }
    }

    def window(collector: MessageCollector, coordinator: TaskCoordinator) {
      throw new FatalException
    }
  }

  /*
   * Helper method used to retrieve the value of a counter from a group.
   */
  private def getCount(
    group: ConcurrentHashMap[String, Metric],
    name: String): Long = {
    group.get("exception-ignored-" + name.toLowerCase).asInstanceOf[Counter].getCount
  }

  /**
   * Test task instance exception metrics with two ignored exceptions and one
   * exception not ignored.
   */
  @Test
  def testExceptionCounts {
    val task = new TroublesomeTask
    val ignoredExceptions = classOf[TroublesomeException].getName + "," +
      classOf[NonFatalException].getName
    val config = new MapConfig(Map[String, String](
      "task.ignored.exceptions" -> ignoredExceptions))

    val partition = new Partition(0)
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val systemStream = new SystemStream("test-system", "test-stream")
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val systemStreamPartitions = Set(systemStreamPartition)
    // Pretend our last checkpointed (next) offset was 2.
    val testSystemStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val offsetManager = OffsetManager(Map(systemStream -> testSystemStreamMetadata), config)
    val taskName = new TaskName("taskName")
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val containerContext = new SamzaContainerContext(0, config, Set[TaskName](taskName))

    val registry = new MetricsRegistryMap
    val taskMetrics = new TaskInstanceMetrics(registry = registry)
    val taskInstance: TaskInstance = new TaskInstance(
      task,
      taskName,
      config,
      taskMetrics,
      null,
      consumerMultiplexer,
      collector,
      containerContext,
      offsetManager,
      systemStreamPartitions = systemStreamPartitions,
      exceptionHandler = TaskInstanceExceptionHandler(taskMetrics, config))

    val coordinator = new ReadableCoordinator(taskName)
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "1", null, null), coordinator)
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "2", null, null), coordinator)
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "3", null, null), coordinator)

    val group = registry.getGroup(taskMetrics.group)
    assertEquals(1L, getCount(group, classOf[TroublesomeException].getName))
    assertEquals(2L, getCount(group, classOf[NonFatalException].getName))

    intercept[FatalException] {
      taskInstance.window(coordinator)
    }
    assertFalse(group.contains(classOf[FatalException].getName.toLowerCase))
  }

  /**
   * Test task instance exception metrics with all exception ignored using a
   * wildcard.
   */
  @Test
  def testIgnoreAllExceptions {
    val task = new TroublesomeTask
    val config = new MapConfig(Map[String, String](
      "task.ignored.exceptions" -> "*"))

    val partition = new Partition(0)
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val systemStream = new SystemStream("test-system", "test-stream")
    val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
    val systemStreamPartitions = Set(systemStreamPartition)
    // Pretend our last checkpointed (next) offset was 2.
    val testSystemStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val offsetManager = OffsetManager(Map(systemStream -> testSystemStreamMetadata), config)
    val taskName = new TaskName("taskName")
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val containerContext = new SamzaContainerContext(0, config, Set[TaskName](taskName))

    val registry = new MetricsRegistryMap
    val taskMetrics = new TaskInstanceMetrics(registry = registry)
    val taskInstance: TaskInstance = new TaskInstance(
      task,
      taskName,
      config,
      taskMetrics,
      null,
      consumerMultiplexer,
      collector,
      containerContext,
      offsetManager,
      systemStreamPartitions = systemStreamPartitions,
      exceptionHandler = TaskInstanceExceptionHandler(taskMetrics, config))

    val coordinator = new ReadableCoordinator(taskName)
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "1", null, null), coordinator)
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "2", null, null), coordinator)
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "3", null, null), coordinator)
    taskInstance.window(coordinator)

    val group = registry.getGroup(taskMetrics.group)
    assertEquals(1L, getCount(group, classOf[TroublesomeException].getName))
    assertEquals(2L, getCount(group, classOf[NonFatalException].getName))
    assertEquals(1L, getCount(group, classOf[FatalException].getName))
  }

  /**
   * Tests that the init() method of task can override the existing offset
   * assignment.
   */
  @Test
  def testManualOffsetReset {

    val partition0 = new SystemStreamPartition("system", "stream", new Partition(0))
    val partition1 = new SystemStreamPartition("system", "stream", new Partition(1))

    val task = new StreamTask with InitableTask {

      override def init(config: Config, context: TaskContext): Unit = {

        assertTrue("Can only update offsets for assigned partition",
          context.getSystemStreamPartitions.contains(partition1))

        context.setStartingOffset(partition1, "10")
      }

      override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {}
    }

    val config = new MapConfig()
    val chooser = new RoundRobinChooser()
    val consumers = new SystemConsumers(chooser, consumers = Map.empty)
    val producers = new SystemProducers(Map.empty, new SerdeManager())
    val metrics = new TaskInstanceMetrics()
    val taskName = new TaskName("Offset Reset Task 0")
    val collector = new TaskInstanceCollector(producers)
    val containerContext = new SamzaContainerContext(0, config, Set[TaskName](taskName))

    val offsetManager = new OffsetManager()

    offsetManager.startingOffsets += taskName -> Map(partition0 -> "0", partition1 -> "0")

    val taskInstance = new TaskInstance(
      task,
      taskName,
      config,
      metrics,
      null,
      consumers,
      collector,
      containerContext,
      offsetManager,
      systemStreamPartitions = Set(partition0, partition1))

    taskInstance.initTask

    assertEquals(Some("0"), offsetManager.getStartingOffset(taskName, partition0))
    assertEquals(Some("10"), offsetManager.getStartingOffset(taskName, partition1))
  }

  @Test
  def testIgnoreMessagesOlderThanStartingOffsets {
    val partition0 = new SystemStreamPartition("system", "stream", new Partition(0))
    val partition1 = new SystemStreamPartition("system", "stream", new Partition(1))
    val config = new MapConfig()
    val chooser = new RoundRobinChooser()
    val consumers = new SystemConsumers(chooser, consumers = Map.empty)
    val producers = new SystemProducers(Map.empty, new SerdeManager())
    val metrics = new TaskInstanceMetrics()
    val taskName = new TaskName("testing")
    val collector = new TaskInstanceCollector(producers)
    val containerContext = new SamzaContainerContext(0, config, Set[TaskName](taskName))
    val offsetManager = new OffsetManager()
    offsetManager.startingOffsets += taskName -> Map(partition0 -> "0", partition1 -> "100")
    val systemAdmins = Map("system" -> new MockSystemAdmin)
    var result = new ListBuffer[IncomingMessageEnvelope]

    val task = new StreamTask {
      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
        result += envelope
      }
    }

    val taskInstance = new TaskInstance(
      task,
      taskName,
      config,
      metrics,
      systemAdmins,
      consumers,
      collector,
      containerContext,
      offsetManager,
      systemStreamPartitions = Set(partition0, partition1))

    val coordinator = new ReadableCoordinator(taskName)
    val envelope1 = new IncomingMessageEnvelope(partition0, "1", null, null)
    val envelope2 = new IncomingMessageEnvelope(partition0, "2", null, null)
    val envelope3 = new IncomingMessageEnvelope(partition1, "1", null, null)
    val envelope4 = new IncomingMessageEnvelope(partition1, "102", null, null)

    taskInstance.process(envelope1, coordinator)
    taskInstance.process(envelope2, coordinator)
    taskInstance.process(envelope3, coordinator)
    taskInstance.process(envelope4, coordinator)

    val expected = List(envelope1, envelope2, envelope4)
    assertEquals(expected, result.toList)
  }
}

class MockSystemAdmin extends SystemAdmin {
  override def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = { offsets }
  override def getSystemStreamMetadata(streamNames: java.util.Set[String]) = null
  override def createCoordinatorStream(stream: String) = {}
  override def createChangelogStream(topicName: String, numKafkaChangelogPartitions: Int) = {}
  override def validateChangelogStream(topicName: String, numOfPartitions: Int) = {}

  override def offsetComparator(offset1: String, offset2: String) = {
    offset1.toLong compare offset2.toLong
  }
}
