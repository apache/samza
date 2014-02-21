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

import java.io.File
import org.apache.samza.config.Config
import org.junit.Assert._
import org.junit.Test
import org.apache.samza.Partition
import org.apache.samza.config.MapConfig
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.SystemConsumers
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemProducers
import org.apache.samza.system.SystemProducer
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.task.StreamTask
import org.apache.samza.task.MessageCollector
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.InitableTask
import org.apache.samza.task.TaskContext
import org.apache.samza.task.ClosableTask
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin
import org.apache.samza.system.SystemStream

class TestSamzaContainer {
  @Test
  def testGetInputStreamMetadata {
    val inputStreams = Set(
      new SystemStreamPartition("test", "stream1", new Partition(0)),
      new SystemStreamPartition("test", "stream1", new Partition(1)),
      new SystemStreamPartition("test", "stream2", new Partition(0)),
      new SystemStreamPartition("test", "stream2", new Partition(1)))
    val systemAdmins = Map("test" -> new SinglePartitionWithoutOffsetsSystemAdmin)
    val metadata = SamzaContainer.getInputStreamMetadata(inputStreams, systemAdmins)
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
    val partition = new Partition(0)
    val containerName = "test-container"
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val taskInstance: TaskInstance = new TaskInstance(
      task,
      partition,
      config,
      new TaskInstanceMetrics,
      consumerMultiplexer: SystemConsumers,
      producerMultiplexer: SystemProducers)
    val container = new SamzaContainer(
      Map(partition -> taskInstance),
      config,
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
