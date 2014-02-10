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

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.system.SystemStream
import org.apache.samza.checkpoint.Checkpoint
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.SamzaException
import org.apache.samza.Partition

class TestCheckpointSerde {
  @Test
  def testExactlyOneOffset {
    val serde = new CheckpointSerde
    var offsets = Map[SystemStream, String]()
    val systemStream = new SystemStream("test-system", "test-stream")
    offsets += systemStream -> "1"
    val deserializedOffsets = serde.fromBytes(serde.toBytes(new Checkpoint(offsets)))
    assertEquals("1", deserializedOffsets.getOffsets.get(systemStream))
    assertEquals(1, deserializedOffsets.getOffsets.size)
  }

  @Test
  def testMoreThanOneOffsetShouldFail {
    val serde = new CheckpointSerde
    var offsets = Map[SystemStream, String]()
    // Since SS != SSP with same system and stream name, this should result in 
    // two offsets for one system stream in the serde.
    offsets += new SystemStream("test-system", "test-stream") -> "1"
    offsets += new SystemStreamPartition("test-system", "test-stream", new Partition(0)) -> "2"
    try {
      serde.toBytes(new Checkpoint(offsets))
      fail("Expected to fail with more than one offset for a single SystemStream.")
    } catch {
      case e: SamzaException => // expected this
    }
  }
}