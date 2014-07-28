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

import org.junit.Test
import org.junit.Assert._
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.{SamzaException, Partition}

class TestTaskNamesToSystemStreamPartitions {
  var sspCounter = 0
  def makeSSP(stream:String) = new SystemStreamPartition("system", stream, new Partition(42))

  @Test
  def toSetWorksCorrectly() {
    val map = Map(new TaskName("tn1") -> Set(makeSSP("tn1-1"), makeSSP("tn1-2")),
                  new TaskName("tn2") -> Set(makeSSP("tn2-1"), makeSSP("tn2-2")))
    val tntssp = TaskNamesToSystemStreamPartitions(map)

    val asSet = tntssp.toSet
    val expected = Set(new TaskName("tn1") -> Set(makeSSP("tn1-1"), makeSSP("tn1-2")),
                      (new TaskName("tn2") -> Set(makeSSP("tn2-1"), makeSSP("tn2-2"))))
    assertEquals(expected , asSet)
  }

  @Test
  def validateMethodCatchesDuplicatedSSPs() {
    val duplicatedSSP1 = new SystemStreamPartition("sys", "str", new Partition(42))
    val duplicatedSSP2 = new SystemStreamPartition("sys", "str", new Partition(42))
    val notDuplicatedSSP1 = new SystemStreamPartition("sys", "str2", new Partition(42))
    val notDuplicatedSSP2 = new SystemStreamPartition("sys", "str3", new Partition(42))

    val badMapping = Map(new TaskName("a") -> Set(notDuplicatedSSP1, duplicatedSSP1), new TaskName("b") -> Set(notDuplicatedSSP2, duplicatedSSP2))

    var caughtException = false
    try {
      TaskNamesToSystemStreamPartitions(badMapping)
    } catch {
      case se: SamzaException => assertEquals("Assigning the same SystemStreamPartition to multiple " +
        "TaskNames is not currently supported.  Out of compliance SystemStreamPartitions and counts: " +
        "Map(SystemStreamPartition [sys, str, 42] -> 2)", se.getMessage)
        caughtException = true
      case _: Throwable       =>
    }
    assertTrue("TaskNamesToSystemStreamPartitions should have rejected this mapping but didn't", caughtException)
  }

  @Test
  def validateMethodAllowsUniqueSSPs() {
    val sspSet1 = (0 to 10).map(p => new SystemStreamPartition("sys", "str", new Partition(p))).toSet
    val sspSet2 = (0 to 10).map(p => new SystemStreamPartition("sys", "str2", new Partition(p))).toSet

    TaskNamesToSystemStreamPartitions(Map(new TaskName("set1") -> sspSet1, new TaskName("set2") -> sspSet2))
  }
}
