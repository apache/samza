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

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemProducers
import org.apache.samza.task.MessageCollector
import org.apache.samza.task.StreamTask
import org.apache.samza.system.SystemConsumers
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.config.MapConfig
import org.apache.samza.Partition
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.system.SystemProducer
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.system.SystemConsumer
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.task.ReadableCoordinator
import org.apache.samza.checkpoint.OffsetManager
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import scala.collection.JavaConversions._
import org.apache.samza.task.TaskInstanceCollector

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
    // Pretend our last checkpointed (next) offset was 2.
    val testSystemStreamMetadata = new SystemStreamMetadata(systemStream.getStream, Map(partition -> new SystemStreamPartitionMetadata("0", "1", "2")))
    val offsetManager = OffsetManager(Map(systemStream -> testSystemStreamMetadata), config)
    val taskName = new TaskName("taskName")
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val taskInstance: TaskInstance = new TaskInstance(
      task,
      taskName,
      config,
      new TaskInstanceMetrics,
      consumerMultiplexer,
      collector,
      offsetManager)
    // Pretend we got a message with offset 2 and next offset 3.
    val coordinator = new ReadableCoordinator(taskName)
    taskInstance.process(new IncomingMessageEnvelope(systemStreamPartition, "2", null, null), coordinator)
    // Check to see if the offset manager has been properly updated with offset 3.
    val lastProcessedOffset = offsetManager.getLastProcessedOffset(systemStreamPartition)
    assertTrue(lastProcessedOffset.isDefined)
    assertEquals("2", lastProcessedOffset.get)
  }
}