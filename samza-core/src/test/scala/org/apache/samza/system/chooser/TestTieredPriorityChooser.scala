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

package org.apache.samza.system.chooser

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.system.IncomingMessageEnvelope
import scala.collection.immutable.Queue
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition
import org.apache.samza.SamzaException
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import java.util.Arrays
import org.apache.samza.system.SystemStream

@RunWith(value = classOf[Parameterized])
class TestTieredPriorityChooser(getChooser: (Map[SystemStream, Int], Map[Int, MessageChooser], MessageChooser) => MessageChooser) {
  val envelope1 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), null, null, 1);
  val envelope2 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(1)), null, null, 2);
  val envelope3 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(0)), null, null, 3);
  val envelope4 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), "123", null, 4);

  @Test
  def testChooserShouldStartStopAndRegister {
    val mock0 = new MockMessageChooser
    val mock1 = new MockMessageChooser
    val chooser = getChooser(
      Map(envelope1.getSystemStreamPartition -> 1),
      Map(1 -> mock1),
      mock0)

    chooser.register(envelope1.getSystemStreamPartition, "foo")
    chooser.start
    assertEquals("foo", mock1.registers(envelope1.getSystemStreamPartition))
    assertEquals(1, mock0.starts)
    assertEquals(1, mock1.starts)
    chooser.stop
    assertEquals(1, mock0.stops)
    assertEquals(1, mock1.stops)
  }

  @Test
  def testChooserShouldFallBackToDefault {
    val mock = new MockMessageChooser
    val chooser = getChooser(
      Map(),
      Map(),
      mock)

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.start
    assertEquals(null, chooser.choose)
    chooser.update(envelope1)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)
  }

  @Test
  def testChooserShouldFailWithNoDefault {
    val mock = new MockMessageChooser
    val chooser = getChooser(
      Map(envelope1.getSystemStreamPartition.getSystemStream -> 0),
      Map(0 -> mock),
      null)

    // The SSP for envelope2 is not defined as a priority stream.
    chooser.register(envelope2.getSystemStreamPartition, null)
    chooser.start
    assertEquals(null, chooser.choose)

    try {
      chooser.update(envelope2)
      fail("Should have failed due to missing default chooser.")
    } catch {
      case e: SamzaException => // Expected.
    }
  }

  @Test
  def testChooserWithSingleStream {
    val mock = new MockMessageChooser
    val chooser = getChooser(
      Map(envelope1.getSystemStreamPartition.getSystemStream -> 0),
      Map(0 -> mock),
      new MockMessageChooser)

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.start

    assertEquals(null, chooser.choose)
    chooser.update(envelope1)
    chooser.update(envelope4)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope4, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope4)
    chooser.update(envelope1)
    assertEquals(envelope4, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)
  }

  @Test
  def testChooserWithSingleStreamWithTwoPartitions {
    val mock = new MockMessageChooser
    val chooser = getChooser(
      Map(envelope2.getSystemStreamPartition.getSystemStream -> 0),
      Map(0 -> mock),
      new MockMessageChooser)

    chooser.register(envelope2.getSystemStreamPartition, null)
    chooser.register(envelope3.getSystemStreamPartition, null)
    chooser.start

    assertEquals(null, chooser.choose)
    chooser.update(envelope2)
    chooser.update(envelope3)
    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope3, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope3)
    chooser.update(envelope2)
    assertEquals(envelope3, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)
  }

  @Test
  def testChooserWithTwoStreamsOfEqualPriority {
    val mock = new MockMessageChooser
    val chooser = getChooser(
      Map(
        envelope1.getSystemStreamPartition.getSystemStream -> 0,
        envelope2.getSystemStreamPartition.getSystemStream -> 0),
      Map(0 -> mock),
      new MockMessageChooser)

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.register(envelope2.getSystemStreamPartition, null)
    chooser.start

    assertEquals(null, chooser.choose)
    chooser.update(envelope1)
    chooser.update(envelope4)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope4, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope4)
    chooser.update(envelope1)
    assertEquals(envelope4, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope2)
    chooser.update(envelope4)
    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope4, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope2)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)
  }

  @Test
  def testChooserWithTwoStreamsOfDifferentPriority {
    val mock0 = new MockMessageChooser
    val mock1 = new MockMessageChooser
    val chooser = getChooser(
      Map(
        envelope1.getSystemStreamPartition.getSystemStream -> 1,
        envelope2.getSystemStreamPartition.getSystemStream -> 0),
      Map(
        0 -> mock0,
        1 -> mock1),
      new MockMessageChooser)

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.register(envelope2.getSystemStreamPartition, null)
    chooser.start

    assertEquals(null, chooser.choose)
    chooser.update(envelope1)
    chooser.update(envelope4)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope4, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope4)
    chooser.update(envelope1)
    assertEquals(envelope4, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope2)
    chooser.update(envelope4)
    // Reversed here because envelope4.SSP=envelope1.SSP which is higher 
    // priority.
    assertEquals(envelope4, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope2)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)

    // Just the low priority stream.
    chooser.update(envelope2)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)
  }
}

object TestTieredPriorityChooser {
  // Test both PriorityChooser and DefaultChooser here. DefaultChooser with 
  // just priorities defined should behave just like plain vanilla priority 
  // chooser.
  @Parameters
  def parameters: java.util.Collection[Array[(Map[SystemStream, Int], Map[Int, MessageChooser], MessageChooser) => MessageChooser]] = Arrays.asList(
    Array((priorities: Map[SystemStream, Int], choosers: Map[Int, MessageChooser], default: MessageChooser) => new TieredPriorityChooser(priorities, choosers, default)),
    Array((priorities: Map[SystemStream, Int], choosers: Map[Int, MessageChooser], default: MessageChooser) => new DefaultChooser(default, None, priorities, choosers)))
}