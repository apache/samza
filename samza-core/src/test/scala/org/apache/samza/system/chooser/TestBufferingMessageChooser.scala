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

import org.apache.samza.system.chooser.ChooserStatus._
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition
import org.junit.Assert._
import org.junit.Test

class TestBufferingMessageChooser {
  @Test
  def testShouldBufferAndFlush {
    val ssp1 = new SystemStreamPartition("test-system", "test-stream", new Partition(0))
    val chooser = new MockMessageChooser
    val buffer = new BufferingMessageChooser(chooser)
    val envelope1 = new IncomingMessageEnvelope(ssp1, "1", null, null)
    buffer.register(ssp1, "1")
    assertEquals(1, chooser.registers.size)
    assertEquals("1", chooser.registers.getOrElse(ssp1, "0"))
    buffer.start
    assertEquals(1, chooser.starts)
    assertEquals(null, buffer.choose)
    buffer.update(envelope1)
    // Should buffer this update, rather than passing it to the wrapped chooser.
    assertEquals(null, buffer.choose)
    buffer.flush
    assertEquals(envelope1, buffer.choose)
    assertEquals(null, buffer.choose)
    buffer.stop
    assertEquals(1, chooser.stops)
  }

  @Test
  def testBufferShouldSkipCheckedSSPs {
    val ssp1 = new SystemStreamPartition("test-system", "test-stream", new Partition(0))
    val chooser = new MockMessageChooser
    val buffer = new BufferingMessageChooser(chooser)
    val envelope1 = new IncomingMessageEnvelope(ssp1, "1", null, null)
    buffer.register(ssp1, "1")
    buffer.start
    checkChooserStatus(NeededByChooser, ssp1, buffer)

    // Buffer first message. Still needed.
    buffer.update(envelope1)
    checkChooserStatus(NeededByChooser, ssp1, buffer)
    assertEquals(null, buffer.choose)
    checkChooserStatus(NeededByChooser, ssp1, buffer)

    // Flush first message. Now in chooser.
    buffer.flush
    checkChooserStatus(InChooser, ssp1, buffer)
    assertEquals(envelope1, buffer.choose)
    checkChooserStatus(NeededByChooser, ssp1, buffer)

    // Now flush with no messages. Should begin skipping chooser since no 
    // messages are available.
    assertEquals(null, buffer.choose)
    checkChooserStatus(NeededByChooser, ssp1, buffer)
    buffer.flush
    checkChooserStatus(SkippingChooser, ssp1, buffer)

    // Now check that we can get back to NeededByChooser when a new message 
    // arrives.
    buffer.update(envelope1)
    checkChooserStatus(NeededByChooser, ssp1, buffer)
    assertEquals(0, chooser.envelopes.size)
    assertEquals(null, buffer.choose)

    // And check that we can get back into the InChooser state.
    buffer.flush
    checkChooserStatus(InChooser, ssp1, buffer)
    assertEquals(envelope1, buffer.choose)
    checkChooserStatus(NeededByChooser, ssp1, buffer)

    // Shutdown.
    buffer.stop
    assertEquals(1, chooser.stops)
  }

  private def checkChooserStatus(status: ChooserStatus, systemStreamPartition: SystemStreamPartition, buffer: BufferingMessageChooser) {
    if (status.equals(NeededByChooser)) {
      assertEquals(Set(systemStreamPartition), buffer.neededByChooser)
    } else {
      assertTrue(!buffer.neededByChooser.contains(systemStreamPartition))
    }

    assertEquals(status, buffer.statuses.getOrElse(systemStreamPartition, null))
  }
}