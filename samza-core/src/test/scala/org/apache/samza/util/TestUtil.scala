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

import org.junit.Test
import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.Util._
import org.junit.Assert._
import org.apache.samza.config.MapConfig
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemFactory
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.config.Config

object TestUtil {
  def expect[T](exception: Class[T], msg: Option[String] = None)(block: => Unit) = try {
    block
  } catch {
    case e: Exception =>
      assertEquals(e.getClass, exception)
      if (msg.isDefined) {
        assertEquals(msg.get, e.getMessage)
      }
  }
}

class TestUtil {
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
  def testCreateStreamPartitionStringBlocksDelimeters() {
    val partOne = new Partition(1)
    val toTry = List(Util.topicSeparator, Util.topicStreamGrouper, Util.partitionSeparator)
      .map(ch => (ch, Set(new SystemStreamPartition("kafka", "good1", partOne),
      new SystemStreamPartition("kafka", "bad" + ch, partOne),
      new SystemStreamPartition("notkafka", "alsogood", partOne))))
    toTry.foreach(t => try {
      createStreamPartitionString(t._2)
      fail("Should have thrown an exception")
    } catch {
      case iae:IllegalArgumentException =>
        val expected = "SystemStreamPartition [partition=Partition [partition=1], system" +
          "=kafka, stream=bad" + t._1 + "] contains illegal character " + t._1
        assertEquals(expected, iae.getMessage)
    } )
  }

  @Test
  def testCreateStreamPartitionStringRoundTrip() {
    val getPartitions = {
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

    val streamsAndParts = getStreamsAndPartitionsForContainer(0, 4, getPartitions)
    println(streamsAndParts)
    val asString = createStreamPartitionString(streamsAndParts)
    println(asString)
    val backToStreamsAndParts = createStreamPartitionsFromString(asString)

    assertEquals(streamsAndParts, backToStreamsAndParts)

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
