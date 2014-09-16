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
package org.apache.samza.job

import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition
import org.apache.samza.util.Util._
import org.apache.samza.container.{TaskName, TaskNamesToSystemStreamPartitions}
import org.junit.Assert._
import org.junit.Test

class TestShellCommandBuilder {

  @Test
  def testJsonCreateStreamPartitionStringRoundTrip() {
    val getPartitions: Set[SystemStreamPartition] = {
      // Build a heavily skewed set of partitions.
      def partitionSet(max:Int) = (0 until max).map(new Partition(_)).toSet
      val system = "all-same-system."
      val lotsOfParts = Map(system + "topic-with-many-parts-a" -> partitionSet(128),
        system + "topic-with-many-parts-b" -> partitionSet(128), system + "topic-with-many-parts-c" -> partitionSet(64))
      val fewParts = ('c' to 'z').map(l => system + l.toString -> partitionSet(4)).toMap
      val streamsMap = (lotsOfParts ++ fewParts)
      (for(s <- streamsMap.keys;
           part <- streamsMap.getOrElse(s, Set.empty)) yield new SystemStreamPartition(getSystemStreamFromNames(s), part)).toSet
    }

    // Group by partition...
    val sspTaskNameMap = TaskNamesToSystemStreamPartitions(getPartitions.groupBy(p => new TaskName(p.getPartition.toString)).toMap)

    val asString = ShellCommandBuilder.serializeSystemStreamPartitionSetToJSON(sspTaskNameMap.getJavaFriendlyType)

    val backFromSSPTaskNameMap = TaskNamesToSystemStreamPartitions(ShellCommandBuilder.deserializeSystemStreamPartitionSetFromJSON(asString))
    assertEquals(sspTaskNameMap, backFromSSPTaskNameMap)
  }
}
