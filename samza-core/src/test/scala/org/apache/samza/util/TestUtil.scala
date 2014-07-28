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
package org.apache.samza.util

import org.apache.samza.Partition
import org.apache.samza.config.Config
import org.apache.samza.config.Config
import org.apache.samza.config.MapConfig
import org.apache.samza.container.{TaskName, TaskNamesToSystemStreamPartitions}
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.Util._
import org.junit.Assert._
import org.junit.Test
import scala.collection.JavaConversions._
import scala.util.Random

class TestUtil {
  val random = new Random(System.currentTimeMillis())

  @Test
  def testGetInputStreamPartitions {
    val expectedPartitionsPerStream = 1
    val inputSystemStreamNames = List("test.foo", "test.bar")
    val config = new MapConfig(Map(
      "task.inputs" -> inputSystemStreamNames.mkString(","),
      "systems.test.samza.factory" -> classOf[MockSystemFactory].getName,
      "systems.test.partitions.per.stream" -> expectedPartitionsPerStream.toString))
    val systemStreamPartitions = Util.getInputStreamPartitions(config)
    assertNotNull(systemStreamPartitions)
    assertEquals(expectedPartitionsPerStream * inputSystemStreamNames.size, systemStreamPartitions.size)
    inputSystemStreamNames.foreach(systemStreamName => {
      (0 until expectedPartitionsPerStream).foreach(partitionNumber => {
        val partition = new Partition(partitionNumber)
        val systemStreamNameSplit = systemStreamName.split("\\.")
        systemStreamPartitions.contains(new SystemStreamPartition(systemStreamNameSplit(0), systemStreamNameSplit(1), partition))
      })
    })
  }

  @Test
  def testResolveTaskNameToChangelogPartitionMapping {
    def testRunner(description:String, currentTaskNames:Set[TaskName], previousTaskNameMapping:Map[TaskName, Int],
                   result:Map[TaskName, Int]) {
      assertEquals("Failed: " + description, result,
        Util.resolveTaskNameToChangelogPartitionMapping(currentTaskNames, previousTaskNameMapping))
    }

    testRunner("No change between runs",
      Set(new TaskName("Partition 0")),
      Map(new TaskName("Partition 0") -> 0),
      Map(new TaskName("Partition 0") -> 0))

    testRunner("New TaskName added, none missing this run",
      Set(new TaskName("Partition 0"), new TaskName("Partition 1")),
      Map(new TaskName("Partition 0") -> 0),
      Map(new TaskName("Partition 0") -> 0, new TaskName("Partition 1") -> 1))

    testRunner("New TaskName added, one missing this run",
      Set(new TaskName("Partition 0"), new TaskName("Partition 2")),
      Map(new TaskName("Partition 0") -> 0, new TaskName("Partition 1") -> 1),
      Map(new TaskName("Partition 0") -> 0, new TaskName("Partition 1") -> 1, new TaskName("Partition 2") -> 2))

    testRunner("New TaskName added, all previous missing this run",
      Set(new TaskName("Partition 3"), new TaskName("Partition 4")),
      Map(new TaskName("Partition 0") -> 0, new TaskName("Partition 1") -> 1, new TaskName("Partition 2") -> 2),
      Map(new TaskName("Partition 0") -> 0, new TaskName("Partition 1") -> 1, new TaskName("Partition 2") -> 2, new TaskName("Partition 3") -> 3, new TaskName("Partition 4") -> 4))
  }

  /**
   * Generate a random alphanumeric string of the specified length
   * @param length Specifies length of the string to generate
   * @return An alphanumeric string
   */
  def generateString (length : Int) : String = {
    Random.alphanumeric.take(length).mkString
  }

  @Test
  def testCompressAndDecompressUtility() {
    var len : Integer = 0
    (10 until 1000).foreach(len => {
      val sample = generateString(len)
      val compressedStr = Util.compress(sample)
      val deCompressedStr = Util.decompress(compressedStr)
      assertEquals(sample, deCompressedStr)
    })
  }
}

/**
 * Little mock for testing the input stream partition method.
 */
class MockSystemFactory extends SystemFactory {
  def getConsumer(systemName: String, config: Config, registry: MetricsRegistry) = null
  def getProducer(systemName: String, config: Config, registry: MetricsRegistry) = null
  def getAdmin(systemName: String, config: Config) = new SinglePartitionWithoutOffsetsSystemAdmin
}
