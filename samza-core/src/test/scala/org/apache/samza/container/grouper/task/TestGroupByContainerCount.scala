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
package org.apache.samza.container.grouper.task

import java.util

import org.apache.samza.container.TaskName
import org.apache.samza.system.SystemStreamPartition
import org.junit.Assert._
import org.junit.Test
import org.apache.samza.job.model.TaskModel
import org.apache.samza.Partition
import org.scalatest.Assertions.intercept
import scala.collection.JavaConversions._

class TestGroupByContainerCount {
  @Test
  def testEmptyTasks {
    intercept[IllegalArgumentException] { new GroupByContainerCount(1).group(new util.HashSet()) }
  }

  @Test
  def testFewerTasksThanContainers {
    val taskModels = new util.HashSet[TaskModel]()
    taskModels.add(getTaskModel("1", 1))
    intercept[IllegalArgumentException] { new GroupByContainerCount(2).group(taskModels) }
  }

  @Test
  def testHappyPath {
    val taskModels = Set(
      getTaskModel("1", 1),
      getTaskModel("2", 2),
      getTaskModel("3", 3),
      getTaskModel("4", 4),
      getTaskModel("5", 5))
    val containers = asScalaSet(new GroupByContainerCount(2)
      .group(setAsJavaSet(taskModels)))
      .map(containerModel => containerModel.getContainerId -> containerModel)
      .toMap
    assertEquals(2, containers.size)
    val container0 = containers(0)
    val container1 = containers(1)
    assertNotNull(container0)
    assertNotNull(container1)
    assertEquals(0, container0.getContainerId)
    assertEquals(1, container1.getContainerId)
    assertEquals(3, container0.getTasks.size)
    assertEquals(2, container1.getTasks.size)
    assertTrue(container0.getTasks.containsKey(new TaskName("1")))
    assertTrue(container0.getTasks.containsKey(new TaskName("3")))
    assertTrue(container0.getTasks.containsKey(new TaskName("5")))
    assertTrue(container1.getTasks.containsKey(new TaskName("2")))
    assertTrue(container1.getTasks.containsKey(new TaskName("4")))
  }

  private def getTaskModel(name: String, partitionId: Int) = {
    new TaskModel(new TaskName(name), Set[SystemStreamPartition](), new Partition(partitionId))
  }
}
