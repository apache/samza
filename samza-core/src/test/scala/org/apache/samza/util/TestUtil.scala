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
import org.apache.samza.config.MapConfig
import org.apache.samza.metrics.MetricsRegistry
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
  def testGetTopicPartitionsForTask() {
    def partitionSet(max:Int) = (0 until max).map(new Partition(_)).toSet

    val taskCount = 4
    val streamsMap = Map("kafka.a" -> partitionSet(4), "kafka.b" -> partitionSet(18), "timestream.c" -> partitionSet(24))
    val streamsAndParts = (for(s <- streamsMap.keys;
                               part <- streamsMap.getOrElse(s, Set.empty))
    yield new SystemStreamPartition(getSystemStreamFromNames(s), part)).toSet

    for(i <- 0 until taskCount) {
      val result: Set[SystemStreamPartition] = Util.getStreamsAndPartitionsForContainer(i, taskCount, streamsAndParts)
      // b -> 18 % 4 = 2 therefore first two results should have an extra element
      if(i < 2) {
        assertEquals(12, result.size)
      } else {
        assertEquals(11, result.size)
      }

      result.foreach(r => assertEquals(i, r.getPartition.getPartitionId % taskCount))
    }
  }
  
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

    val streamsAndParts: Set[SystemStreamPartition] = getStreamsAndPartitionsForContainer(0, 4, getPartitions).toSet
    println(streamsAndParts)
    val asString = serializeSSPSetToJSON(streamsAndParts)

    val backToStreamsAndParts = deserializeSSPSetFromJSON(asString)
    assertEquals(streamsAndParts, backToStreamsAndParts)
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
