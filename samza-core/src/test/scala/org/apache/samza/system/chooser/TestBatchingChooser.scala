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
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import java.util.Arrays

@RunWith(value = classOf[Parameterized])
class TestBatchingChooser(getChooser: (MessageChooser, Int) => MessageChooser) {
  @Test
  def testChooserShouldHandleBothBatchSizeOverrunAndNoEnvelopeAvailable {
    val envelope1 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), null, null, 1);
    val envelope2 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(1)), null, null, 2);
    val envelope3 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), null, null, 3);
    val mock = new MockMessageChooser
    val chooser = getChooser(mock, 2)

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.register(envelope2.getSystemStreamPartition, "")
    chooser.start
    // Make sure start and register are working.
    assertEquals(1, mock.starts)
    assertEquals(null, mock.registers(envelope1.getSystemStreamPartition))
    assertEquals("", mock.registers(envelope2.getSystemStreamPartition))
    assertEquals(null, chooser.choose)
    chooser.update(envelope1)
    assertEquals(envelope1, mock.getEnvelopes.head)
    assertEquals(envelope1, chooser.choose)
    assertEquals(None, mock.getEnvelopes.headOption)
    chooser.update(envelope2)
    assertEquals(envelope2, mock.getEnvelopes.head)
    // This envelope should be batched, and therefore cached in the BatchingChooser.
    chooser.update(envelope3)
    assertEquals(1, mock.getEnvelopes.size)
    assertEquals(envelope2, mock.getEnvelopes.head)
    // Preferred envelope3 over envelope2, since envelope3 was in the same SSP.
    assertEquals(envelope3, chooser.choose)
    // Since batch size is 2, we should have reset batch size, and should fall back to the mock now.
    chooser.update(envelope1)
    assertEquals(2, mock.getEnvelopes.size)
    assertEquals(envelope2, mock.getEnvelopes.head)
    assertEquals(envelope1, mock.getEnvelopes.last)
    assertEquals(envelope2, chooser.choose)
    assertEquals(1, mock.getEnvelopes.size)
    assertEquals(envelope1, mock.getEnvelopes.head)
    // Now envelope 2's SSP (kafka.stream1, partition 1) is preferred, but no new envelopes for this partition have been loaded.
    // Let's trigger a reset back to envelope2's SSP (kafka.stream, partition 0).
    assertEquals(envelope1, chooser.choose)
    assertEquals(0, mock.getEnvelopes.size)
    // Now verify that SSP (kafka.stream1, partition 1) is preferred again.
    chooser.update(envelope2)
    chooser.update(envelope1)
    assertEquals(1, mock.getEnvelopes.size)
    assertEquals(envelope2, mock.getEnvelopes.head)
    assertEquals(envelope1, chooser.choose)
    chooser.stop
    assertEquals(1, mock.stops)
  }
}

object TestBatchingChooser {
  // Test both BatchingChooser and DefaultChooser here. DefaultChooser with 
  // just batch size defined should behave just like plain vanilla batching 
  // chooser.
  @Parameters
  def parameters: java.util.Collection[Array[(MessageChooser, Int) => MessageChooser]] = Arrays.asList(
    Array((wrapped: MessageChooser, batchSize: Int) => new BatchingChooser(wrapped, batchSize)),
    Array((wrapped: MessageChooser, batchSize: Int) => new DefaultChooser(wrapped, Some(batchSize))))
}