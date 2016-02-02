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
import org.junit.{After, Before, Test}
import org.apache.samza.SamzaException
import org.apache.samza.Partition
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.container.TaskName
import org.junit.rules.TemporaryFolder

class TestFileSystemCheckpointManager  {
  val checkpointRoot = System.getProperty("java.io.tmpdir") // TODO: Move this out of tmp, into our build dir
  val taskName = new TaskName("Warwickshire")
  val baseFileLocation = new File(checkpointRoot)

  val tempFolder = new TemporaryFolder

  @Before
  def createTempFolder = tempFolder.create()

  @After
  def deleteTempFolder = tempFolder.delete()

  @Test
  def testReadForCheckpointFileThatDoesNotExistShouldReturnNull {
    val cpm = new FileSystemCheckpointManager("some-job-name", tempFolder.getRoot)
    assertNull(cpm.readLastCheckpoint(taskName))
  }

  @Test
  def testReadForCheckpointFileThatDoesExistShouldReturnProperCheckpoint {
    val cp = new Checkpoint(Map(
      new SystemStreamPartition("a", "b", new Partition(0)) -> "c",
      new SystemStreamPartition("a", "c", new Partition(1)) -> "d",
      new SystemStreamPartition("b", "d", new Partition(2)) -> "e"))

    var readCp:Checkpoint = null
    val cpm =  new FileSystemCheckpointManager("some-job-name", tempFolder.getRoot)

    cpm.start
    cpm.writeCheckpoint(taskName, cp)
    readCp = cpm.readLastCheckpoint(taskName)
    cpm.stop

    assertNotNull(readCp)
    cp.equals(readCp)
    assertEquals(cp.getOffsets.keySet(), readCp.getOffsets.keySet())
    assertEquals(cp.getOffsets, readCp.getOffsets)
    assertEquals(cp, readCp)
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