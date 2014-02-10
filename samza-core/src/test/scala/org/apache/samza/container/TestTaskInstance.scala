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

class TestTaskInstance {
  @Test
  def testOffsetsAreUpdatedOnProcess {
    val task = new StreamTask {
      def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
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
    val systemStream = new SystemStream("test-system", "test-stream")
    // Pretend our last checkpointed offset was 1.
    taskInstance.offsets += systemStream -> "1"
    // Pretend we got a message with offset 2.
    taskInstance.process(new IncomingMessageEnvelope(new SystemStreamPartition("test-system", "test-stream", new Partition(0)), "2", null, null), new ReadableCoordinator)
    // Check to see if the offset map has been properly updated with offset 2.
    assertEquals(1, taskInstance.offsets.size)
    assertEquals("2", taskInstance.offsets(systemStream))
  }
}