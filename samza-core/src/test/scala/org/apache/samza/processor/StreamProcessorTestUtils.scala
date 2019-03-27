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

import java.util

import org.apache.samza.Partition
import org.apache.samza.config.MapConfig
import org.apache.samza.container._
import org.apache.samza.context.{ContainerContext, JobContext}
import org.apache.samza.job.model.TaskModel
import org.apache.samza.serializers.SerdeManager
import org.apache.samza.storage.ContainerStorageManager
import org.apache.samza.system._
import org.apache.samza.system.chooser.RoundRobinChooser
import org.apache.samza.task.{StreamTask, TaskInstanceCollector}
import org.mockito.Mockito


object StreamProcessorTestUtils {
  def getDummyContainer(mockRunloop: RunLoop, streamTask: StreamTask) = {
    val config = new MapConfig()
    val taskName = new TaskName("taskName")
    val taskModel = new TaskModel(taskName, new util.HashSet[SystemStreamPartition](), new Partition(0))
    val adminMultiplexer = new SystemAdmins(config)
    val consumerMultiplexer = new SystemConsumers(
      new RoundRobinChooser,
      Map[String, SystemConsumer](), SystemAdmins.empty())
    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)
    val collector = new TaskInstanceCollector(producerMultiplexer)
    val containerContext = Mockito.mock(classOf[ContainerContext])
    val taskInstance: TaskInstance = new TaskInstance(
      streamTask,
      taskModel,
      new TaskInstanceMetrics,
      adminMultiplexer,
      consumerMultiplexer,
      collector,
      jobContext = Mockito.mock(classOf[JobContext]),
      containerContext = containerContext,
      applicationContainerContextOption = None,
      applicationTaskContextFactoryOption = None,
      externalContextOption = None)

    val container = new SamzaContainer(
      config = config,
      taskInstances = Map(taskName -> taskInstance),
      taskInstanceMetrics = Map(taskName -> new TaskInstanceMetrics),
      runLoop = mockRunloop,
      systemAdmins = adminMultiplexer,
      consumerMultiplexer = consumerMultiplexer,
      producerMultiplexer = producerMultiplexer,
      metrics = new SamzaContainerMetrics,
      containerContext = containerContext,
      applicationContainerContextOption = None,
      externalContextOption = None,
      containerStorageManager = Mockito.mock(classOf[ContainerStorageManager]))
    container
  }
}