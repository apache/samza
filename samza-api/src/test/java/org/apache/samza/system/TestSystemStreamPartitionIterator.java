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

package org.apache.samza.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.samza.Partition;
import org.junit.Test;

public class TestSystemStreamPartitionIterator {
  private static final SystemStreamPartition SSP = new SystemStreamPartition("test", "test", new Partition(0));

  @Test
  public void testHasNextShouldWork() {
    int numMessages = 10;
    MockSystemConsumer consumer = new MockSystemConsumer(numMessages);
    SystemStreamPartitionIterator iterator = new SystemStreamPartitionIterator(consumer, SSP);

    while (iterator.hasNext()) {
      assertEquals(--numMessages, iterator.next().getMessage());
    }

    assertFalse(iterator.hasNext());
    assertEquals(0, numMessages);
  }

  @Test
  public void testNextWithoutHasNextCallShouldWorkWhenAvailableAndFailWhenNot() {
    int numMessages = 10;
    MockSystemConsumer consumer = new MockSystemConsumer(numMessages);
    SystemStreamPartitionIterator iterator = new SystemStreamPartitionIterator(consumer, SSP);

    for (int i = 0; i < numMessages; ++i) {
      assertEquals(numMessages - i - 1, iterator.next().getMessage());
    }

    assertFalse(iterator.hasNext());

    try {
      iterator.next();
      fail("Expected not to get any more messages from iterator.");
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  @Test
  public void testNoMessages() {
    int numMessages = 0;
    MockSystemConsumer consumer = new MockSystemConsumer(numMessages);
    SystemStreamPartitionIterator iterator = new SystemStreamPartitionIterator(consumer, SSP);

    assertFalse(iterator.hasNext());

    try {
      iterator.next();
      fail("Expected not to get any more messages from iterator.");
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  public class MockSystemConsumer implements SystemConsumer {
    private int numPollReturnsWithMessages;

    public MockSystemConsumer(int numMessages) {
      this.numPollReturnsWithMessages = numMessages;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String startingOffset) {
    }

    @Override
    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> systemStreamPartitionEnvelopes = new HashMap<SystemStreamPartition, List<IncomingMessageEnvelope>>();

      for (SystemStreamPartition systemStreamPartition : systemStreamPartitions) {
        List<IncomingMessageEnvelope> q = new ArrayList<IncomingMessageEnvelope>();

        if (numPollReturnsWithMessages-- > 0) {
          q.add(new IncomingMessageEnvelope(SSP, "", null, numPollReturnsWithMessages));
        }

        systemStreamPartitionEnvelopes.put(systemStreamPartition, q);
      }

      return systemStreamPartitionEnvelopes;
    }
  }
}
