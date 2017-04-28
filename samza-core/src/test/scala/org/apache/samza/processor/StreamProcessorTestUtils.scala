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
package org.apache.samza.processor

import java.util.Collections

import org.apache.samza.config.MapConfig
import org.apache.samza.container.{RunLoop, SamzaContainer, SamzaContainerContext, SamzaContainerMetrics, TaskInstance, TaskInstanceMetrics, TaskName}
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.system.{SystemConsumer, SystemConsumers, SystemProducer, SystemProducers}
import org.apache.samza.task.{StreamTask, TaskInstanceCollector}


object StreamProcessorTestUtils {
  def getDummyContainer(mockRunloop: RunLoop, containerListener: SamzaContainerListener, streamTask: StreamTask) = {
    val config = new MapConfig
    val taskName = new TaskName("taskName")
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer]())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val containerContext = new SamzaContainerContext("0", config, Collections.singleton[TaskName](taskName))
    val taskInstance: TaskInstance = new TaskInstance(
      streamTask,
      taskName,
      config,
      new TaskInstanceMetrics,
      null,
      consumerMultiplexer,
      collector,
      containerContext
    )

    val container = new SamzaContainer(
      containerContext = containerContext,
      taskInstances = Map(taskName -> taskInstance),
      runLoop = mockRunloop,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = new SamzaContainerMetrics,
      jmxServer = null)
    if (containerListener != null) {
      container.setContainerListener(containerListener)
    }
    container
  }
}