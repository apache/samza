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

package org.apache.samza.checkpoint.file

import java.io.File
import scala.collection.JavaConversions._
import java.util.Random
import org.junit.Assert._
import org.junit.Test
import org.apache.samza.SamzaException
import org.apache.samza.Partition
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.system.SystemStream

class TestFileSystemCheckpointManager {
  val checkpointRoot = System.getProperty("java.io.tmpdir")

  @Test
  def testReadForCheckpointFileThatDoesNotExistShouldReturnNull {
    val cpm = new FileSystemCheckpointManager("some-job-name", new File(checkpointRoot))
    assert(cpm.readLastCheckpoint(new Partition(1)) == null)
  }

  @Test
  def testReadForCheckpointFileThatDoesExistShouldReturnProperCheckpoint {
    val cpm = new FileSystemCheckpointManager("some-job-name", new File(checkpointRoot))
    val partition = new Partition(2)
    val cp = new Checkpoint(Map(
      new SystemStream("a", "b") -> "c",
      new SystemStream("a", "c") -> "d",
      new SystemStream("b", "d") -> "e"))
    cpm.start
    cpm.writeCheckpoint(partition, cp)
    val readCp = cpm.readLastCheckpoint(partition)
    cpm.stop
    assert(readCp.equals(cp))
  }

  @Test
  def testMissingRootDirectoryShouldFailOnManagerCreation {
    val cpm = new FileSystemCheckpointManager("some-job-name", new File(checkpointRoot + new Random().nextInt))
    try {
      cpm.start
      fail("Expected an exception since root directory for fs checkpoint manager doesn't exist.")
    } catch {
      case e: SamzaException => None // this is expected
    }
    cpm.stop
  }
}
