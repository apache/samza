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

import org.apache.samza.container.{TaskName, TaskNamesToSystemStreamPartitions}
import org.apache.samza.system.SystemStreamPartition
import org.junit.Assert._
import org.junit.Test

class TestGroupByContainerCount {
  val emptySSPSet = Set[SystemStreamPartition]()

  @Test
  def weGetAsExactlyManyGroupsAsWeAskFor() {
    // memoize the maps used in the test to avoid an O(n^3) loop
    val tntsspCache = scala.collection.mutable.Map[Int, TaskNamesToSystemStreamPartitions]()

    def tntsspOfSize(size:Int) = {
      def getMap(size:Int) = TaskNamesToSystemStreamPartitions((0 until size).map(z => new TaskName("tn" + z) -> emptySSPSet).toMap)

      tntsspCache.getOrElseUpdate(size, getMap(size))
    }

    val maxTNTSSPSize = 1000
    val maxNumGroups = 140
    for(numGroups <- 1 to maxNumGroups) {
      val grouper = new GroupByContainerCount(numGroups)

      for (tntsspSize <- numGroups to maxTNTSSPSize) {
        val map = tntsspOfSize(tntsspSize)
        assertEquals(tntsspSize, map.size)

        val grouped = grouper.groupTaskNames(map)
        assertEquals("Asked for " + numGroups + " but got " + grouped.size, numGroups, grouped.size)
      }
    }
  }
}
