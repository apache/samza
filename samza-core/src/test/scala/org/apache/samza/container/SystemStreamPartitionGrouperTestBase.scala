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

import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamPartition
import org.junit.Test
import java.util.HashSet
import java.util.Map
import java.util.Set
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import java.util.Collections

object SystemStreamPartitionGrouperTestBase {
  val aa0 = new SystemStreamPartition("SystemA", "StreamA", new Partition(0))
  val aa1 = new SystemStreamPartition("SystemA", "StreamA", new Partition(1))
  val aa2 = new SystemStreamPartition("SystemA", "StreamA", new Partition(2))
  val ab1 = new SystemStreamPartition("SystemA", "StreamB", new Partition(1))
  val ab2 = new SystemStreamPartition("SystemA", "StreamB", new Partition(2))
  val ac0 = new SystemStreamPartition("SystemA", "StreamB", new Partition(0))
  val allSSPs = new HashSet[SystemStreamPartition]
  Collections.addAll(allSSPs, aa0, aa1, aa2, ab1, ab2, ac0)
}

abstract class SystemStreamPartitionGrouperTestBase {
  def getGrouper: SystemStreamPartitionGrouper

  @Test
  def emptySetReturnsEmptyMap {
    val grouper: SystemStreamPartitionGrouper = getGrouper
    val result: Map[TaskName, Set[SystemStreamPartition]] = grouper.group(new HashSet[SystemStreamPartition])
    assertTrue(result.isEmpty)
  }

  def verifyGroupGroupsCorrectly(input: Set[SystemStreamPartition], output: Map[TaskName, Set[SystemStreamPartition]]) {
    val grouper: SystemStreamPartitionGrouper = getGrouper
    val result: Map[TaskName, Set[SystemStreamPartition]] = grouper.group(input)
    assertEquals(output, result)
  }
}