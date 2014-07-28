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
package org.apache.samza.container.systemstreampartition.taskname.groupers

import org.apache.samza.container.{TaskName, SystemStreamPartitionTaskNameGrouper, TaskNamesToSystemStreamPartitions}
import org.apache.samza.system.SystemStreamPartition

/**
 * Group the SSP taskNames by dividing the number of taskNames into the number of containers (n) and assigning n taskNames
 * to each container as returned by iterating over the keys in the map of taskNames (whatever that ordering happens to be).
 * No consideration is given towards locality, even distribution of aggregate SSPs within a container, even distribution
 * of the number of taskNames between containers, etc.
 */
class SimpleSystemStreamPartitionTaskNameGrouper(numContainers:Int) extends SystemStreamPartitionTaskNameGrouper {
  require(numContainers > 0, "Must have at least one container")

  override def groupTaskNames(taskNames: TaskNamesToSystemStreamPartitions): Map[Int, TaskNamesToSystemStreamPartitions] = {
    val keySize = taskNames.keySet.size
    require(keySize > 0, "Must have some SSPs to group, but found none")

    // Iterate through the taskNames, round-robining them per container
    val byContainerNum = (0 until numContainers).map(_ -> scala.collection.mutable.Map[TaskName, Set[SystemStreamPartition]]()).toMap
    var idx = 0
    for(taskName <- taskNames.iterator) {
      val currMap = byContainerNum.get(idx).get // safe to use simple get since we populated everybody above
      idx = (idx + 1) % numContainers

      currMap += taskName
    }

    byContainerNum.map(kv => kv._1 -> TaskNamesToSystemStreamPartitions(kv._2)).toMap
  }
}

