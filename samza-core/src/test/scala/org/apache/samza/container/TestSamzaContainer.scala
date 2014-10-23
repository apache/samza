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

import org.apache.samza.config.Config
import org.junit.Assert._
import org.junit.Test
import org.apache.samza.Partition
import org.apache.samza.config.MapConfig
import org.apache.samza.metrics.JmxServer
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemConsumers
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemProducers
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemStream
import org.apache.samza.system.StreamMetadataCache
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.task.StreamTask
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.InitableTask
import org.apache.samza.task.TaskContext
import org.apache.samza.task.ClosableTask
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin
import org.scalatest.junit.AssertionsForJUnit
import org.apache.samza.coordinator.server.HttpServer
import org.apache.samza.coordinator.server.JobServlet
import scala.collection.JavaConversions._

class TestSamzaContainer extends AssertionsForJUnit {
  @Test
  def testCoordinatorObjects {
    val server = new HttpServer("/test")
    try {
      val taskName = new TaskName("a")
      val set = Set(new SystemStreamPartition("a", "b", new Partition(0)))
      val config = new MapConfig(Map("a" -> "b", "c" -> "d"))
      val containerToTaskMapping = Map(0 -> new TaskNamesToSystemStreamPartitions(Map(taskName -> set)))
      val taskToChangelogMapping = Map[TaskName, Int](taskName -> 0)
      server.addServlet("/job", new JobServlet(config, containerToTaskMapping, taskToChangelogMapping))
      server.start
      val (returnedConfig, returnedSspTaskNames, returnedTaskNameToChangeLogPartitionMapping) = SamzaContainer.getCoordinatorObjects(server.getUrl.toString + "/job")
      assertEquals(config, returnedConfig)
      assertEquals(containerToTaskMapping, returnedSspTaskNames)
      assertEquals(taskToChangelogMapping, returnedTaskNameToChangeLogPartitionMapping)
    } finally {
      server.stop
    }
  }

  @Test
  def testJmxServerShutdownOnException {
    var stopped = false
    val jmxServer = new JmxServer {
      override def stop {
        super.stop
        stopped = true
      }
    }
    intercept[Exception] {
      // Calling main will trigger an NPE since the container checks for an
      // isCompressed environment variable, which isn't set.
      SamzaContainer.safeMain(jmxServer)
    }
    assertTrue(stopped)
  }

  @Test
  def testGetInputStreamMetadata {
    val inputStreams = Set(
      new SystemStreamPartition("test", "stream1", new Partition(0)),
      new SystemStreamPartition("test", "stream1", new Partition(1)),
      new SystemStreamPartition("test", "stream2", new Partition(0)),
      new SystemStreamPartition("test", "stream2", new Partition(1)))
    val systemAdmins = Map("test" -> new SinglePartitionWithoutOffsetsSystemAdmin)
    val metadata = new StreamMetadataCache(systemAdmins).getStreamMetadata(inputStreams.map(_.getSystemStream).toSet)
    assertNotNull(metadata)
    assertEquals(2, metadata.size)
    val stream1Metadata = metadata(new SystemStream("test", "stream1"))
    val stream2Metadata = metadata(new SystemStream("test", "stream2"))
    assertNotNull(stream1Metadata)
    assertNotNull(stream2Metadata)
    assertEquals("stream1", stream1Metadata.getStreamName)
    assertEquals("stream2", stream2Metadata.getStreamName)
  }

  @Test
  def testExceptionInTaskInitShutsDownTask {
    val task = new StreamTask with InitableTask with ClosableTask {
      var wasShutdown = false

      def init(config: Config, context: TaskContext) {
        throw new Exception("Trigger a shutdown, please.")
      }

      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
      }

      def close {
        wasShutdown = true
      }
    }
    val config = new MapConfig
    val taskName = new TaskName("taskName")
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val taskInstance: TaskInstance = new TaskInstance(
      task,
      taskName,
      config,
      new TaskInstanceMetrics,
      consumerMultiplexer,
      collector)
    val runLoop = new RunLoop(
      taskInstances = Map(taskName -> taskInstance),
      consumerMultiplexer = consumerMultiplexer,
      metrics = new SamzaContainerMetrics)
    val container = new SamzaContainer(
      Map(taskName -> taskInstance),
      runLoop,
      consumerMultiplexer,
      producerMultiplexer,
      new SamzaContainerMetrics)
    try {
      container.run
      fail("Expected exception to be thrown in run method.")
    } catch {
      case e: Exception => // Expected
    }
    assertTrue(task.wasShutdown)
  }
}
