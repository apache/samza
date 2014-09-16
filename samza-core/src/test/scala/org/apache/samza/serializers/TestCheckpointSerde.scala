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

package org.apache.samza.serializers

import java.util

import org.apache.samza.Partition
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.container.TaskName
import org.apache.samza.system.SystemStreamPartition
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class TestCheckpointSerde {
  @Test
  def testExactlyOneOffset {
    val serde = new CheckpointSerde
    var offsets = Map[SystemStreamPartition, String]()
    val systemStreamPartition = new SystemStreamPartition("test-system", "test-stream", new Partition(777))
    offsets += systemStreamPartition -> "1"
    val deserializedOffsets = serde.fromBytes(serde.toBytes(new Checkpoint(offsets)))
    assertEquals("1", deserializedOffsets.getOffsets.get(systemStreamPartition))
    assertEquals(1, deserializedOffsets.getOffsets.size)
  }

  @Test
  def testChangelogPartitionMappingRoundTrip {
    val mapping = new util.HashMap[TaskName, java.lang.Integer]()
    mapping.put(new TaskName("Ted"), 0)
    mapping.put(new TaskName("Dougal"), 1)
    mapping.put(new TaskName("Jack"), 2)

    val checkpointSerde = new CheckpointSerde
    val asBytes = checkpointSerde.changelogPartitionMappingToBytes(mapping)
    val backToMap = checkpointSerde.changelogPartitionMappingFromBytes(asBytes)

    assertEquals(mapping, backToMap)
    assertNotSame(mapping, backToMap)
  }

}
