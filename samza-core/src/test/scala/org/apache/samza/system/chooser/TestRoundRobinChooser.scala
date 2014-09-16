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

import java.util.Arrays

import org.apache.samza.Partition
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemStreamPartition
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(value = classOf[Parameterized])
class TestRoundRobinChooser(getChooser: () => MessageChooser) {
  @Test
  def testRoundRobinChooser {
    val chooser = getChooser()
    val envelope1 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), null, null, 1);
    val envelope2 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(1)), null, null, 2);
    val envelope3 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(0)), null, null, 3);

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.register(envelope2.getSystemStreamPartition, "")
    chooser.register(envelope3.getSystemStreamPartition, "123")
    chooser.start

    assertNull(chooser.choose)

    // Test one message.
    chooser.update(envelope1)
    assertEquals(envelope1, chooser.choose)
    assertNull(chooser.choose)

    // Verify simple ordering.
    chooser.update(envelope1)
    chooser.update(envelope2)
    chooser.update(envelope3)

    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope3, chooser.choose)
    assertNull(chooser.choose)

    // Verify mixed ordering.
    chooser.update(envelope2)
    chooser.update(envelope1)

    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope1, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope2)

    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertNull(chooser.choose)

    // Verify simple ordering with different starting envelope.
    chooser.update(envelope2)
    chooser.update(envelope1)
    chooser.update(envelope3)

    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope3, chooser.choose)
    assertNull(chooser.choose)
  }
}

object TestRoundRobinChooser {
  // Test both RoundRobinChooser and DefaultChooser here. DefaultChooser with 
  // no batching, prioritization, or bootstrapping should default to just a 
  // plain vanilla round robin chooser.
  @Parameters
  def parameters: java.util.Collection[Array[() => MessageChooser]] = Arrays.asList(Array(() => new RoundRobinChooser), Array(() => new DefaultChooser))
}
